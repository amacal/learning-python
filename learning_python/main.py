import os
import time
import click
import base64
import hashlib
import requests
import itertools

from typing import Set
from typing import Dict
from typing import List
from typing import Tuple
from typing import Optional
from typing import Iterable

from datetime import datetime


HASH_LENGTH = int(os.environ.get("HASH_LENGTH", 40))


class CodeLine:
    def __init__(self, index: int, value: str, hash: str):
        self._index = index
        self._value = value
        self._hash = hash

    def encode(self) -> bytes:
        return self._value.encode("utf-8")

    def length(self) -> int:
        return len(self._value)

    def get_index(self) -> int:
        return self._index

    def get_hash(self) -> str:
        return self._hash

    def get_value(self) -> str:
        return self._value


class CodeLineCollection:
    def __init__(self, lines: List[CodeLine]):
        self._lines = lines

    @staticmethod
    def create(lines: List[str]) -> "CodeLineCollection":
        return CodeLineCollection([CodeLine(index, line, _hash(line)) for index, line in enumerate(lines)])

    @staticmethod
    def merge(entries: List["CodeLineCollection"]) -> "CodeLineCollection":
        lines = [line for entry in entries for line in entry.get_lines()]
        result = CodeLineCollection(sorted(lines, key=lambda line: line.get_index()))

        return result

    def first(self) -> CodeLine:
        return self._lines[0]

    def last(self) -> CodeLine:
        return self._lines[-1]

    def length(self) -> int:
        return len(self._lines)

    def characters(self) -> int:
        return sum(line.length() for line in self._lines)

    def get_lines(self) -> List[CodeLine]:
        return self._lines

    def get_hash(self) -> str:
        return _hash("".join(line.get_value() for line in self._lines))

    def unique(self) -> "CodeLineCollection":
        groups: Dict[str, List[CodeLine]] = dict()

        for line in self.get_lines():
            if line.get_hash() not in groups:
                groups[line.get_hash()] = [line]
            else:
                groups[line.get_hash()].append(line)

        lines = [lines[0] for _, lines in groups.items() if len(lines) == 1]
        return CodeLineCollection(sorted(lines, key=lambda line: line.get_index()))


class CodeBoundary:
    def __init__(self, before: str, last: str):
        self._before = before
        self._last = last

    def get_start(self) -> str:
        return self._before

    def get_end(self) -> str:
        return self._last


class CodeChunkRef:
    def __init__(self, start: str, end: str, hash: str, timestamp: str):
        self._start = start
        self._end = end
        self._hash = hash
        self._timestamp = timestamp

    @staticmethod
    def parse(line: str) -> "CodeChunkRef":
        return CodeChunkRef(
            line[0:HASH_LENGTH],
            line[HASH_LENGTH : 2 * HASH_LENGTH],
            line[2 * HASH_LENGTH : 3 * HASH_LENGTH],
            line[3 * HASH_LENGTH : 3 * HASH_LENGTH + 10],
        )

    def id(self) -> str:
        return "".join([self.get_start(), self.get_end(), self.get_hash(), self._timestamp])

    def get_start(self) -> str:
        return self._start

    def get_end(self) -> str:
        return self._end

    def get_hash(self) -> str:
        return self._hash

    def get_timestamp(self) -> str:
        return self._timestamp


class CodeChunk:
    def __init__(self, boundary: CodeBoundary, lines: CodeLineCollection, timestamp: Optional[str] = None):
        self._boundary = boundary
        self._lines = lines
        self._timestamp = timestamp

    def id(self, timestamp: Optional[str] = None) -> str:
        return "".join([self.get_start(), self.get_end(), self.get_hash(), timestamp or self._timestamp or ""])

    def get_start(self) -> str:
        return self._boundary.get_start()

    def get_end(self) -> str:
        return self._boundary.get_end()

    def get_hash(self) -> str:
        return self._lines.get_hash()

    def get_lines(self) -> List[CodeLine]:
        return self._lines.get_lines()

    def get_characters(self) -> int:
        return self._lines.characters()

    def describe(self) -> str:
        return f"{self.get_characters()} {self._lines.first().get_index()} {self._lines.last().get_index()}"

    @staticmethod
    def merge(chunks: List["CodeChunk"], timestamp: Optional[str] = None) -> "CodeChunk":
        boundary = CodeBoundary(chunks[0]._boundary._before, chunks[-1]._boundary._last)
        lines = CodeLineCollection.merge([chunk._lines for chunk in chunks])

        return CodeChunk(boundary, lines, timestamp)


class CodeChunkCollection:
    def __init__(self, chunks: List[CodeChunk]):
        self._chunks = chunks

    def iterate(self) -> Iterable[CodeChunk]:
        return self._chunks

    def split(self, length: int) -> "CodeChunkCollection":
        def separate(chunks: List[CodeChunk]) -> List[List[CodeChunk]]:
            result: List[List[CodeChunk]] = list()

            if chunks:
                result.append(list([chunks[0]]))

            for chunk in chunks[1:]:
                if chunk.get_start() == result[-1][-1].get_end():
                    result[-1].append(chunk)
                else:
                    result.append(list([chunk]))

            return result

        def process(chunks: List[CodeChunk]) -> List[CodeChunk]:
            index, taken = 0, 0

            while taken < length and index < len(chunks):
                taken += chunks[index].get_characters()
                index += 1

            if index == len(chunks):
                return [CodeChunk.merge(chunks)]

            index, taken = 0, 0
            pivot = sum(chunk.get_characters() for chunk in chunks) // 2

            while taken < pivot and index < len(chunks):
                taken += chunks[index].get_characters()
                index += 1

            left = CodeChunkCollection(chunks[:index]).split(length)
            right = CodeChunkCollection(chunks[index:]).split(length)

            return left._chunks + right._chunks

        chunks = [chunk for chunks in separate(self._chunks) for chunk in process(chunks)]
        return CodeChunkCollection(chunks)

    def find(self, ref: CodeChunkRef) -> Optional[CodeChunk]:
        start = [index for index, chunk in enumerate(self._chunks) if chunk.get_start() == ref.get_start()]
        end = [index for index, chunk in enumerate(self._chunks) if chunk.get_end() == ref.get_end()]

        if len(start) == 1 and len(end) == 1:
            consistent = True
            for index in range(start[0], end[0]):
                consistent = consistent and self._chunks[index].get_end() == self._chunks[index + 1].get_start()

            if consistent:
                chunk = CodeChunk.merge(self._chunks[start[0] : end[0] + 1], ref.get_timestamp())
                if chunk.get_hash() == ref.get_hash():
                    return chunk

        return None

    def extract(self, refs: List[CodeChunkRef]) -> Tuple["CodeChunkCollection", "CodeChunkCollection"]:
        def find(ref: CodeChunkRef, chunks: List[CodeChunk]) -> Tuple[Optional[CodeChunk], List[CodeChunk]]:
            start = [index for index, chunk in enumerate(chunks) if chunk.get_start() == ref.get_start()]
            end = [index for index, chunk in enumerate(chunks) if chunk.get_end() == ref.get_end()]

            if len(start) == 1 and len(end) == 1:
                consistent = True
                for index in range(start[0], end[0]):
                    consistent = consistent and chunks[index].get_end() == chunks[index + 1].get_start()

                if consistent:
                    chunk = CodeChunk.merge(chunks[start[0] : end[0] + 1], ref.get_timestamp())
                    if chunk.get_hash() == ref.get_hash():
                        return chunk, chunks[: start[0]] + chunks[end[0] + 1 :]

            return None, chunks

        remainder: List[CodeChunk] = self._chunks
        collected: List[CodeChunk] = list()

        for ref in refs:
            found, remainder = find(ref, remainder)

            if found:
                collected.append(found)

        return CodeChunkCollection(collected), CodeChunkCollection(remainder)


class CodeDiff:
    def __init__(self, lines: List[str]):
        self._lines = lines

    @staticmethod
    def open(path: str) -> "CodeDiff":
        with open(path) as file:
            return CodeDiff([line.strip() for line in file.readlines()])

    @staticmethod
    def create(chunks: CodeChunkCollection, timestamp: str) -> "CodeDiff":
        def format(chunk: CodeChunk):
            return f"{chunk.get_start()}{chunk.get_end()}{chunk.get_hash()}{timestamp} M {chunk.describe()}"

        return CodeDiff([format(chunk) for chunk in chunks.iterate()])

    def contains(self, line: CodeChunkRef) -> bool:
        return line.id() in [line.id() for line in self.iterate()]

    def merge(self, other: "CodeDiff") -> "CodeDiff":
        return CodeDiff(self._lines + other._lines).reconstruct()

    def reconstruct(self) -> "CodeDiff":
        accepted: Dict[str, CodeChunkRef] = dict()
        rejected: List[CodeChunkRef] = list()

        reconstructed: List[CodeChunkRef] = list()
        chunks: Iterable[CodeChunkRef] = reversed(sorted(self.iterate(), key=lambda x: x.get_timestamp()))

        def contains(chunk: CodeChunkRef) -> bool:
            for item in list(accepted.values()) + rejected:
                if item.get_start() == chunk.get_start() or item.get_end() == chunk.get_end():
                    return True

            return False

        for _, chunks in itertools.groupby(chunks, key=lambda x: x.get_timestamp()):
            for chunk in chunks:
                if not contains(chunk):
                    accepted[chunk.get_start()] = chunk
                else:
                    rejected.append(chunk)

        current = accepted[_first()]
        reconstructed.append(current)

        while current.get_end() != _last():
            current = accepted[current.get_end()]
            reconstructed.append(current)

        return CodeDiff([line.id() for line in reconstructed])

    def iterate(self) -> Iterable[CodeChunkRef]:
        return [CodeChunkRef.parse(line) for line in self._lines]

    def extract(self, chunks: CodeChunkCollection) -> Tuple[CodeChunkCollection, CodeChunkCollection]:
        return chunks.extract([CodeChunkRef.parse(line) for line in self._lines])


class CodeFile:
    def __init__(self, lines: List[str]):
        self._lines = lines

    @staticmethod
    def open(path: str) -> "CodeFile":
        with open(path) as file:
            return CodeFile(file.readlines())

    def _split(self, lines: List[CodeLine], unique: List[CodeLine]) -> Iterable[CodeChunk]:
        previous: str = _first()
        start: Optional[CodeLine] = lines[0]
        latest: Optional[CodeLineCollection] = None

        for line in unique:
            if start:
                latest = CodeLineCollection(lines[start.get_index() : line.get_index() + 1])
                next = lines[latest.last().get_index() + 1] if len(lines) > line.get_index() + 1 else None

                boundary = CodeBoundary(previous, _last() if line == unique[-1] and not next else line.get_hash())
                yield CodeChunk(boundary, latest)

                previous = boundary.get_end()
                start = next

        if start:
            boundary = CodeBoundary(previous, _last())
            latest = CodeLineCollection(lines[start.get_index() :])

            yield CodeChunk(boundary, latest)

    def chunk(self) -> CodeChunkCollection:
        lines = CodeLineCollection.create(self._lines)
        chunks = self._split(lines.get_lines(), lines.unique().get_lines())

        return CodeChunkCollection(list(chunks))


def _first() -> str:
    return "0" * HASH_LENGTH


def _last() -> str:
    return "f" * HASH_LENGTH


def _hash(line: Optional[str] = None) -> str:
    if line:
        value = hashlib.sha1(line.encode()).hexdigest()
    else:
        value = hashlib.sha1().hexdigest()

    return value[:HASH_LENGTH]


@click.argument("path")
@click.option("--verbose", is_flag=True)
@click.command()
def chunk(path: str, verbose: bool) -> None:
    timestamp = str(int(time.time()))

    for chunk in CodeFile.open(path).chunk().split(10240).iterate():
        print(f"{chunk.id(timestamp)} U {chunk.describe()}")

        if verbose:
            for line in chunk.get_lines():
                print("|", line.get_index(), line.get_value(), end="")


@click.argument("diff")
@click.argument("path")
@click.option("--verbose", is_flag=True)
@click.command()
def diff(path: str, diff: str, verbose: bool) -> None:
    timestamp = str(int(time.time()))
    chunks = CodeFile.open(path).chunk()
    matched, unmatched = CodeDiff.open(diff).extract(chunks)

    for chunk in matched.iterate():
        print(f"{chunk.id()} M {chunk.describe()}")

        if verbose:
            for line in chunk.get_lines():
                print("|", line.get_index(), line.get_value(), end="")

    for chunk in unmatched.split(10240).iterate():
        print(f"{chunk.id(timestamp)} U {chunk.describe()}")

        if verbose:
            for line in chunk.get_lines():
                print("|", line.get_index(), line.get_value(), end="")


@click.argument("diff", required=True)
@click.argument("path", required=True)
@click.option("--verbose", is_flag=True)
@click.command()
def reconstruct(path: str, diff: str, verbose: bool) -> None:
    chunks = CodeFile.open(path).chunk()

    for ref in CodeDiff.open(diff).reconstruct().iterate():
        if chunk := chunks.find(ref):
            print(f"{chunk.id()} U {chunk.describe()}")

            if verbose:
                for line in chunk.get_lines():
                    print("|", line.get_index(), line.get_value(), end="")
        else:
            print(ref.id())


@click.argument("path", required=True)
@click.argument("repo", required=True)
@click.command()
def simulate(repo: str, path: str) -> None:
    recorded: Set[str] = set()
    diff: Optional[CodeDiff] = None

    headers = {"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}"}

    response = requests.get(f"https://api.github.com/repos/{repo}/commits?path={path}", headers=headers)
    response.raise_for_status()

    for entry in reversed(response.json()):
        if entry["commit"]["committer"]["date"] not in recorded:
            recorded.add(entry["commit"]["committer"]["date"])
            timestamp = datetime.strptime(entry["commit"]["committer"]["date"], "%Y-%m-%dT%H:%M:%SZ")

            print()
            print(entry["sha"], entry["commit"]["committer"]["date"], int(timestamp.timestamp()))

            response = requests.get(
                f"https://api.github.com/repos/{repo}/contents/{path}?ref={entry['sha']}", headers=headers
            )
            response.raise_for_status()

            if diff is None:
                code = CodeFile(base64.b64decode(response.json()["content"]).decode("utf-8").splitlines(keepends=True))
                diff = CodeDiff.create(code.chunk().split(10240), str(int(timestamp.timestamp())))
                diff_m = diff
            else:
                code = CodeFile(base64.b64decode(response.json()["content"]).decode("utf-8").splitlines(keepends=True))
                _, unmatched = diff.extract(code.chunk())

                diff_m = CodeDiff.create(unmatched.split(10240), str(int(timestamp.timestamp())))
                diff = diff.merge(diff_m)

            for chunk in diff.iterate():
                print(chunk.id(), "U" if diff_m.contains(chunk) else "")

            time.sleep(1)
