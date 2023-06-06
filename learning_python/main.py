import os
import os.path

import time
import json
import click
import random
import string
import itertools

from io import TextIOWrapper
from queue import PriorityQueue

from typing import Any
from typing import Set
from typing import Dict
from typing import List
from typing import Tuple
from typing import Callable
from typing import Iterator
from typing import Iterable
from typing import Optional
from typing import Protocol

from learning_python.statistics import DataStatistics

from learning_python.types import get_datatype
from learning_python.types import DataType
from learning_python.types import StringType
from learning_python.types import IntegerType

from learning_python.utils import create_if_absent
from learning_python.utils import list_directory
from learning_python.utils import read_lines
from learning_python.utils import read_binary
from learning_python.utils import write_lines
from learning_python.utils import write_binary
from learning_python.utils import split_path
from learning_python.utils import combine_path
from learning_python.utils import as_iterable


class DataSource(Protocol):
    def decode(self) -> Iterator[Any]:
        ...

    def statistic(self, name: str) -> Optional[str]:
        ...

    def statistics(self) -> DataStatistics:
        ...


class DataOutput(Protocol):
    def encode(self, values: Iterator[Any]) -> None:
        ...


class FileDataSource:
    def __init__(self, path: str, type: DataType, statistics: Dict[str, str]):
        self._path = path
        self._type = type
        self._statistics = statistics

    def decode(self) -> Iterator[Any]:
        return self._type.decode(read_binary(self._path), self._statistics)

    def statistic(self, name: str) -> Optional[str]:
        return self._statistics.get(name)

    def statistics(self) -> DataStatistics:
        return DataStatistics(self._statistics)


class DataChunk(Protocol):
    def rows(self, columns: List[str]) -> Iterator[Tuple]:
        ...

    def distinct(self, columns: List[str]) -> Iterator[Tuple]:
        ...

    def statistic(self, column: str, name: str) -> Optional[str]:
        ...

    def statistics(self, column: str) -> DataStatistics:
        ...


class ColumnarChunk:
    def __init__(self, entries: Dict[str, DataSource]):
        self._entries = entries

    def rows(self, columns: List[str]) -> Iterator[Tuple]:
        decoded = [self._entries[column].decode() for column in columns]

        try:
            while True:
                yield tuple(iterator.__next__() for iterator in decoded)
        except RuntimeError:
            yield from []

    def distinct(self, columns: List[str]) -> Iterator[Tuple]:
        processed: Set[Tuple] = set()

        for row in self.rows(columns):
            if row not in processed:
                processed.add(row)
                yield row

    def statistic(self, column: str, name: str) -> Optional[str]:
        return self._entries[column].statistic(name)

    def statistics(self, column: str) -> DataStatistics:
        return self._entries[column].statistics()


class RowBasedChunk:
    def __init__(self, columns: List[str], rows: Iterable[Tuple], statistics: Dict[str, Dict[str, str]]):
        self._rows = rows
        self._columns = columns
        self._statistics = statistics

    def rows(self, columns: List[str]) -> Iterator[Tuple]:
        def reorder(row: Tuple, indices: List[int]) -> Tuple:
            return tuple(row[index] for index in indices)

        indices = [columns.index(column) for column in columns]
        yield from (reorder(row, indices) for row in self._rows)

    def distinct(self, columns: List[str]) -> Iterator[Tuple]:
        processed: Set[Tuple] = set()

        for row in self.rows(columns):
            if row not in processed:
                processed.add(row)
                yield row

    def statistic(self, column: str, name: str) -> Optional[str]:
        return self._statistics[column].get(name)

    def statistics(self, column: str) -> DataStatistics:
        return DataStatistics(self._statistics[column])


class TempWriter(Protocol):
    def __len__(self) -> int:
        ...

    def split(self) -> "TempWriter":
        ...

    def write(self, row: Tuple) -> int:
        ...

    def write_all(self, rows: Iterator[Tuple]) -> int:
        ...

    def distinct(self, deeper: Callable[[Iterator[DataChunk]], Iterator[DataChunk]]) -> Iterator[DataChunk]:
        ...


class MultiFileTempWriter:
    def __init__(self, writers: List["SingleFileTempWriter"]):
        self._writers = writers
        self._count = sum(len(writer) for writer in writers)

    def __len__(self) -> int:
        return self._count

    def split(self) -> "TempWriter":
        return MultiFileTempWriter(
            self._writers
            + [SingleFileTempWriter(self._writers[0]._path + f"+{len(self._writers)}", self._writers[0]._columns)]
        )

    def write(self, row: Tuple) -> int:
        self._count += 1
        return self._writers[-1].write(row)

    def write_all(self, rows: Iterator[Tuple]) -> int:
        before = len(self._writers[-1])
        after = self._writers[-1].write_all(rows)

        self._count += after - before
        return self._count

    def distinct(self, deeper: Callable[[Iterator[DataChunk]], Iterator[DataChunk]]) -> Iterator[DataChunk]:
        def iterate() -> Iterator[DataChunk]:
            for writer in self._writers:
                yield from writer.distinct(deeper)

        return deeper(iterate())


class SingleFileTempWriter:
    def __init__(self, path: str, columns: List[Tuple[str, DataType]]):
        self._path = path
        self._columns = columns

        self._count = 0
        self._file = open(path, "w")

    def __len__(self) -> int:
        return self._count

    def split(self) -> "TempWriter":
        return MultiFileTempWriter([self])

    def write(self, row: Tuple) -> int:
        self._count += 1
        self._file.write(json.dumps(list(row)) + "\n")

        return self._count

    def write_all(self, rows: Iterator[Tuple]) -> int:
        for row in rows:
            self._count += 1
            self._file.write(json.dumps(list(row)) + "\n")

        return self._count

    def readonly(self) -> TextIOWrapper:
        self._file.flush()
        self._file.close()

        return open(self._path, "r")

    def delete(self) -> None:
        os.remove(self._path)

    def distinct(self, deeper: Callable[[Iterator[DataChunk]], Iterator[DataChunk]]) -> Iterator[DataChunk]:
        try:
            with self.readonly() as input:
                processed: Set[Tuple] = set()
                while line := input.readline():
                    if row := tuple(json.loads(line)):
                        if row not in processed:
                            processed.add(row)

            def extract_statistics(type: DataType, index: int) -> Dict[str, str]:
                return type.statistics(as_iterable(lambda: (row[index] for row in processed)))

            columns = [item[0] for item in self._columns]
            statistics = {item[0]: extract_statistics(item[1], i) for i, item in enumerate(self._columns)}

            yield RowBasedChunk(columns, processed, statistics)

        finally:
            self.delete()

    @staticmethod
    def order_by(
        writers: List["SingleFileTempWriter"],
        columns: List[str],
        indices: List[int],
        statistics: Dict[str, DataStatistics],
        batch: int,
    ) -> Iterator[DataChunk]:
        rows: List[Tuple] = list()
        readers = [(len(writer) - 1, writer.readonly(), writer.delete) for writer in writers]
        queue: PriorityQueue[Tuple[Tuple, int, Tuple]] = PriorityQueue(len(writers))

        def reorder(row: Tuple, indices: List[int]) -> Tuple:
            return tuple(row[index] for index in indices)

        for index, reader in enumerate(list(readers)):
            row = tuple(json.loads(reader[1].readline()))
            queue.put((reorder(row, indices), index, row))

        while not queue.empty():
            _, index, row = queue.get()
            rows.append(row)

            if readers[index][0]:
                row = tuple(json.loads(readers[index][1].readline()))
                queue.put((reorder(row, indices), index, row))
                readers[index] = (readers[index][0] - 1, *readers[index][1:])

            if len(rows) >= batch:
                yield RowBasedChunk(columns, rows, {key: value.as_dict() for key, value in statistics.items()})
                rows = list()

        for reader in readers:
            reader[2]()

        if rows:
            yield RowBasedChunk(columns, rows, {key: value.as_dict() for key, value in statistics.items()})


class FileSystemTempDir:
    def __init__(self, root: str):
        self._root = root

    @staticmethod
    def open(root: str) -> "FileSystemTempDir":
        create_if_absent(root)
        return FileSystemTempDir(root)

    def acquire(self) -> str:
        timestamp = int(time.time() * 1000)
        randomized = "".join(random.choice(string.ascii_lowercase) for _ in range(10))

        return os.path.join(self._root, f"{timestamp}-{randomized}")


class QueryPipeline:
    def __init__(self, temp: FileSystemTempDir, columns: Dict[str, DataType], chunks: Iterator[DataChunk]):
        self._columns = columns
        self._chunks = chunks
        self._temp = temp

    def select(self, column: str, *columns: str) -> "QueryPipeline":
        combined = [column] + list(columns)
        reduced = {key: value for key, value in self._columns.items() if key in combined}

        return QueryPipeline(self._temp, reduced, self._chunks)

    def rows(self, column: str, *columns: str) -> Iterator[Tuple]:
        for chunk in self._chunks:
            yield from chunk.rows([column] + list(columns))

    def count(self, column: Optional[str] = None) -> int:
        column_name = column or list(self._columns.keys())[0]
        statistic_name = "count" if column else "rows"

        def count(chunk: DataChunk) -> int:
            if value := chunk.statistic(column_name, statistic_name):
                return int(value)

            return sum(row[0] if column else 1 for row in chunk.rows([column_name]))

        return sum(count(chunk) for chunk in self._chunks)

    def distinct(self, modulos: List[int], batch: int, column: str, *columns: str) -> "QueryPipeline":
        combined = list([column] + list(columns))
        restricted = {key: value for key, value in self._columns.items() if key in combined}

        def acquire_writer(path: str) -> TempWriter:
            return SingleFileTempWriter(path, [(c, self._columns[c]) for c in combined])

        def distinct(chunks: Iterator[DataChunk], modulos: List[int]) -> Iterator[DataChunk]:
            modulo = modulos[0]
            buckets = [acquire_writer(self._temp.acquire()) for _ in range(modulo)]

            for chunk in chunks:
                for row in chunk.distinct(combined):
                    if buckets[hash(row) % modulo].write(row) >= batch:
                        buckets[hash(row) % modulo] = buckets[hash(row) % modulo].split()

            for items in (bucket.distinct(lambda chunks: distinct(chunks, modulos[1:])) for bucket in buckets):
                yield from items

        return QueryPipeline(self._temp, restricted, distinct(self._chunks, modulos))

    def order_by(self, batch: int, column: str, *columns: str) -> "QueryPipeline":
        combined = list([column] + list(columns))
        current = list(self._columns.keys())

        writers: List[SingleFileTempWriter] = list()
        rows: List[Tuple[Tuple, Tuple]] = list()
        statistics: List[Dict[str, DataStatistics]] = list()

        shape = list(self._columns.keys())
        indices = [current.index(column) for column in combined]

        def reorder(row: Tuple, indices: List[int]) -> Tuple:
            return tuple(row[index] for index in indices)

        def acquire_writer(path: str) -> SingleFileTempWriter:
            return SingleFileTempWriter(path, [(c, self._columns[c]) for c in combined])

        def write_all(rows: List[Tuple[Tuple, Tuple]]) -> SingleFileTempWriter:
            writer = acquire_writer(self._temp.acquire())
            writer.write_all(row[1] for row in rows)

            return writer

        for chunk in self._chunks:
            for row in chunk.rows(shape):
                if len(rows) >= batch:
                    rows = sorted(rows)
                    writers.append(write_all(rows))
                    rows = list()

                rows.append((reorder(row, indices), row))
            statistics.append({column: chunk.statistics(column) for column in shape})

        if rows:
            rows = sorted(rows)
            writers.append(write_all(rows))

        statistics_combined = {
            column: DataStatistics.combine([item[column] for item in statistics], only=["type", "min", "max"])
            for column in statistics[0].keys()
        }

        return QueryPipeline(
            self._temp,
            self._columns,
            SingleFileTempWriter.order_by(writers, shape, indices, statistics_combined, batch),
        )


class FileSystemDataDir:
    def __init__(self, root: str):
        self._root = root

    @staticmethod
    def open(root: str) -> "FileSystemDataDir":
        create_if_absent(root)
        return FileSystemDataDir(root)

    def write(self, timestamp: int, column_name: str, statistics: Dict[str, str], data: bytes) -> str:
        attributes = ("timestamp", timestamp, "column", column_name)
        path = combine_path("data", *attributes, **statistics)

        return write_binary(os.path.join(self._root, path), data)


class FileSystemMetaDir:
    def __init__(
        self,
        root: str,
        timestamp: Optional[int],
        items: Dict[str, Dict[str, str]],
        entries: Dict[str, List[str]],
    ):
        self._root = root
        self._items = items
        self._entries = entries
        self._timestamp = timestamp

    @staticmethod
    def open(root: str) -> "FileSystemMetaDir":
        create_if_absent(root)

        paths = (split_path(path) for path in list_directory(root))
        items = {statistics["timestamp"]: statistics for _, statistics in paths}

        newest = max(items.keys()) if items else None
        lines = read_lines(os.path.join(root, combine_path("meta", "timestamp", newest))) if newest else []

        processed = ((split_path(line)[1]["timestamp"], line) for line in lines)
        entries = {key: [row[1] for row in rows] for key, rows in itertools.groupby(processed, lambda line: line[0])}

        return FileSystemMetaDir(root, int(newest) if newest else None, items, entries)

    def describe(self) -> Dict[str, DataType]:
        items = (split_path(path)[1] for path in self._entries[str(self._timestamp)])
        return {item["column"]: get_datatype(item["type"]) for item in items}

    def iterate(self) -> Iterator[DataChunk]:
        for key in sorted(self._entries.keys()):
            yield ColumnarChunk(
                {
                    item[0]["column"]: FileDataSource(item[1], get_datatype(item[0]["type"]), item[0])
                    for item in ((split_path(path)[1], path) for path in self._entries[key])
                }
            )

    def write(self, timestamp: int, file_names: List[str]) -> None:
        path = combine_path("meta", "timestamp", timestamp)
        final = os.path.join(self._root, path)

        self._timestamp = timestamp
        self._entries[str(timestamp)] = sorted(file_names)
        self._items[str(timestamp)] = split_path(path)[1]

        def iterate() -> Iterator[str]:
            for key in sorted(self._entries.keys()):
                yield from self._entries[key]

        write_lines(final, list(iterate()))


class TimestampProvider:
    def acquire(self) -> int:
        return int(time.time() * 1000)


class DataBatch:
    def __init__(self, columns: Dict[str, DataType], entries: List[Dict[str, str]]):
        self._columns = columns
        self._entries = entries

    def get_column_names(self) -> List[str]:
        return sorted(self._columns.keys())

    def encode(self, column_name: str) -> Tuple[bytes, Dict[str, str]]:
        iterable = as_iterable(lambda: (entry[column_name] for entry in self._entries))
        return self._columns[column_name].encode(iterable)


class DataTable:
    def __init__(
        self,
        time: TimestampProvider,
        data: FileSystemDataDir,
        meta: FileSystemMetaDir,
        temp: FileSystemTempDir,
    ):
        self._time = time
        self._data = data
        self._meta = meta
        self._temp = temp

    @staticmethod
    def open(time: TimestampProvider, data_path: str, meta_path: str, temp_path: str) -> "DataTable":
        return DataTable(
            time,
            FileSystemDataDir.open(data_path),
            FileSystemMetaDir.open(meta_path),
            FileSystemTempDir.open(temp_path),
        )

    def ingest(self, batch: DataBatch) -> None:
        timestamp = self._time.acquire()
        column_names = batch.get_column_names()

        def ingest(column_name: str) -> str:
            data, statistics = batch.encode(column_name)
            return self._data.write(timestamp, column_name, statistics, data)

        self._meta.write(timestamp, [ingest(column_name) for column_name in column_names])

    def query(self) -> QueryPipeline:
        return QueryPipeline(self._temp, self._meta.describe(), self._meta.iterate())


@click.command()
def ingest() -> None:
    def read_file(name):
        return (json.loads(line) for line in read_lines(name))

    time = TimestampProvider()
    table = DataTable.open(time, "/wikipedia/revisions/data", "/wikipedia/revisions/meta", "/wikipedia/revisions/temp")

    columns: Dict[str, DataType] = {
        "timestamp": StringType(),
        "page_id": IntegerType(4),
        "page_title": StringType(),
        "namespace_id": StringType(),
        "revision_id": IntegerType(8),
        "revision_sha1": StringType(),
        "contributor_id": StringType(),
        "contributor_name": StringType(),
        "contributor_ip": StringType(),
    }

    total = 0
    files = [os.path.join("/wikipedia", file) for file in os.listdir("/wikipedia") if file.endswith(".json")]

    for index, rows in enumerate([read_file(file) for file in files]):
        items = list(rows)
        total += len(items)

        print(f"Ingesting {index} {len(items)} / {total} ...")
        table.ingest(DataBatch(columns, items))
        print(f"Ingesting {index} {len(items)} / {total} ok")


@click.command()
def count() -> None:
    time = TimestampProvider()
    table = DataTable.open(time, "/wikipedia/revisions/data", "/wikipedia/revisions/meta", "/wikipedia/revisions/temp")

    print(table.query().count())


@click.command()
def rows() -> None:
    time = TimestampProvider()
    table = DataTable.open(time, "/wikipedia/revisions/data", "/wikipedia/revisions/meta", "/wikipedia/revisions/temp")

    for i, value in enumerate(table.query().rows("timestamp", "page_id", "revision_id", "page_title")):
        print(i, value)


@click.command()
def distinct():
    time = TimestampProvider()
    table = DataTable.open(time, "/wikipedia/revisions/data", "/wikipedia/revisions/meta", "/wikipedia/revisions/temp")
    modulos = [17, 19, 23, 29, 31, 37, 39, 41, 43, 47, 53, 57, 59]

    for i, value in enumerate(
        table.query().distinct(modulos, 25000, "page_id", "page_title").rows("page_id", "page_title")
    ):
        print(i, value)


@click.command()
def order_by():
    time = TimestampProvider()
    table = DataTable.open(time, "/wikipedia/revisions/data", "/wikipedia/revisions/meta", "/wikipedia/revisions/temp")

    output = ["page_id", "page_title"]
    modulos = [17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59]

    for i, value in enumerate(
        table.query().select(*output).distinct(modulos, 25000, *output).order_by(25000, "page_title").rows(*output)
    ):
        print(i, value)
