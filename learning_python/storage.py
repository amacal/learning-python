import os
import os.path
import itertools
import urllib.parse

import json
import time

from typing import Any
from typing import Set
from typing import Dict
from typing import List
from typing import Tuple
from typing import Callable
from typing import Optional
from typing import Iterator


class FileVersion:
    def __init__(self, version: str, flags: List[str]):
        self._version = version
        self._flags = flags

    def __str__(self) -> str:
        return f"{self._version}+{''.join(sorted(self._flags))}" if self._flags else self._version

    def get_version(self) -> str:
        return self._version

    def get_flags(self) -> str:
        return self._flags

    def is_newer(self, challenger: "FileVersion") -> bool:
        if self._version > challenger._version:
            return True

        if self._version < challenger._version:
            return False

        for flag in challenger._flags:
            if flag not in self._flags:
                return False

        return True


def create_if_absent(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def list_directory(path: str) -> Iterator[Tuple[str, FileVersion]]:
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                filename, extension = os.path.splitext(entry.name)
                version, flags = (extension.split("+") + [""])[0:2]
                yield (filename, FileVersion(version, flags))
            elif entry.is_dir():
                yield from list_directory(entry.path)


def rewrite_url(url: str, width: int) -> str:
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)

    params["w"] = [str(width)]
    encoded = urllib.parse.urlencode(params, doseq=True)

    return urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, encoded, parsed.fragment)
    )


class Storage:
    def __init__(
        self, root: str, visited: Dict[str, FileVersion], downloaded: Dict[str, Set[str]], versions: Dict[str, int]
    ):
        self._root = root
        self._visited = visited
        self._downloaded = downloaded
        self._versions = versions

    @staticmethod
    def open(root: str, widths: Tuple[int]) -> "Storage":
        create_if_absent(root)
        create_if_absent(os.path.join(root, "queue"))
        create_if_absent(os.path.join(root, "visited"))
        create_if_absent(os.path.join(root, "downloaded"))

        for width in widths:
            create_if_absent(os.path.join(root, "downloaded", str(width)))

        explore_downloaded = lambda width: set(
            [filename for filename, _ in list_directory(os.path.join(root, "downloaded", width))]
        )

        visited = {filename: extension for filename, extension in list_directory(os.path.join(root, "visited"))}
        downloaded = {str(width): explore_downloaded(str(width)) for width in widths}

        versions = {
            key: sum(1 for _ in group)
            for key, group in itertools.groupby(sorted(map(lambda x: str(x), visited.values())))
        }

        return Storage(root, visited, downloaded, versions)

    def is_visited(self, id: str) -> bool:
        return id in self._visited

    def visited_count(self, version: Optional[str] = None) -> int:
        return self._versions[version] if version is not None else len(self._visited)

    def downloaded_count(self, width: int) -> int:
        return len(self._downloaded[str(width)])

    def get_versions(self) -> List[str]:
        return sorted([version for version in self._versions.keys()])

    def get_downloadable(
        self, count: int, width: int, ignorable: Optional[Set[str]] = None
    ) -> Iterator[Tuple[str, str]]:
        for index, id in enumerate(list(self._visited.keys() - self._downloaded[str(width)])):
            if index < count and (not ignorable or id not in ignorable):
                filename = f"{id}{self._visited[id]}"
                filepath = os.path.join(self._root, "visited", id[0:4], filename)

                try:
                    with open(filepath, "r") as file:
                        data: Dict[str, str] = json.load(file)
                        yield id, rewrite_url(data["url"], width)
                except:
                    del self._visited[id]
                    os.remove(filepath)

    def set_visited(
        self,
        id: str,
        url: str,
        flags: str,
        kwargs: Dict[str, Optional[str]],
        merge: Callable[[Dict[str, Any], Dict[str, Any]], Tuple[Dict[str, Any], str]],
    ) -> bool:
        version = ".v5"
        previous: Optional[Dict[str, Any]] = None

        if id in self._visited and self._visited[id].is_newer(FileVersion(version, flags)):
            return False

        if id in self._visited:
            with open(os.path.join(self._root, "visited", id[0:4], f"{id}{self._visited[id]}"), "r") as file:
                previous = json.load(file)

        data: Dict[str, Any] = {"url": url, **kwargs}
        data, flags = merge(previous, data) if previous else (data, flags)
        current = FileVersion(version, flags)

        create_if_absent(os.path.join(self._root, "visited", id[0:4]))
        with open(os.path.join(self._root, "visited", id[0:4], f"{id}{current}"), "w") as file:
            json.dump(data, file, indent=4)

        if previous and str(self._visited[id]) != str(current):
            os.remove(os.path.join(self._root, "visited", id[0:4], f"{id}{self._visited[id]}"))
            self._versions[str(self._visited[id])] -= 1

        if previous:
            if not self._versions[str(self._visited[id])]:
                del self._versions[str(self._visited[id])]

        if str(current) not in self._versions:
            self._versions[str(current)] = 0

        self._visited[id] = current
        self._versions[str(current)] += 1

        return True

    def set_downloaded(self, width: int, id: str, data: bytes) -> None:
        create_if_absent(os.path.join(self._root, "downloaded", str(width), id[0:4]))
        with open(os.path.join(self._root, "downloaded", str(width), id[0:4], f"{id}.png"), "wb") as file:
            self._downloaded[str(width)].add(id)
            file.write(data)

    def push_batch(self, batch: Dict[str, str]) -> None:
        filename = str(int(time.time()))[0:9]
        filepath = os.path.join(self._root, "queue", filename[0:7], filename)

        create_if_absent(os.path.join(self._root, "queue", filename[0:7]))

        if os.path.exists(filepath):
            with open(filepath, "r") as file:
                existing = json.load(file)
                batch = batch | existing

        with open(filepath, "w") as file:
            json.dump(batch, file, indent=4)

    def pop_batch(self) -> Optional[Dict[str, str]]:
        def list(path):
            with os.scandir(path) as entries:
                for entry in entries:
                    if entry.is_file():
                        with open(entry.path, "r") as file:
                            content = json.load(file)
                            os.remove(entry.path)
                            return content
                    if entry.is_dir():
                        if result := list(entry.path):
                            return result

                        os.rmdir(entry.path)

            return None

        return list(os.path.join(self._root, "queue"))
