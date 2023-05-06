import os
import os.path

import json
import time

from typing import Set
from typing import Dict
from typing import Tuple
from typing import Optional
from typing import Iterator


def create_if_absent(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def list_directory(path: str) -> Iterator[str]:
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                yield os.path.splitext(entry.name)[0]
            elif entry.is_dir():
                yield from list_directory(entry.path)


class Storage:
    def __init__(self, root: str, visited: Set[str], downloaded: Set[str]):
        self._root = root
        self._visited = visited
        self._downloaded = downloaded

    @staticmethod
    def open(root: str) -> "Storage":
        create_if_absent(root)
        create_if_absent(os.path.join(root, "queue"))
        create_if_absent(os.path.join(root, "visited"))
        create_if_absent(os.path.join(root, "downloaded"))

        visited = set(list_directory(os.path.join(root, "visited")))
        downloaded = set(list_directory(os.path.join(root, "downloaded")))

        return Storage(root, visited, downloaded)

    def is_visited(self, id: str) -> bool:
        return id in self._visited

    def is_downloaded(self, id: str) -> bool:
        return id in self._downloaded
    
    def visited_count(self) -> int:
        return len(self._visited)

    def downloaded_count(self) -> int:
        return len(self._downloaded)
    
    def get_downloadable(self) -> Iterator[Tuple[str, str]]:
        for id in list(self._visited - self._downloaded):
            try:
                with open(os.path.join(self._root, "visited", id[0:4], id), 'r') as file:
                    data: Dict[str, str] = json.load(file)
                    yield id, data["url"]
            except:
                self._visited.remove(id)
                os.remove(os.path.join(self._root, "visited", id[0:4], id))
    
    def set_visited(self, id: str, url: str) -> None:
        create_if_absent(os.path.join(self._root, "visited", id[0:4]))
        with open(os.path.join(self._root, "visited", id[0:4], id), 'w') as file:
            json.dump({"url": url}, file, indent=4)
            self._visited.add(id)

    def set_downloaded(self, id: str, data: bytes) -> None:
        create_if_absent(os.path.join(self._root, "downloaded", id[0:4]))
        with open(os.path.join(self._root, "downloaded", id[0:4], f"{id}.png"), 'wb') as file:
            self._downloaded.add(id)
            file.write(data)

    def push_batch(self, batch: Dict[str, str]) -> None:
        filename = str(int(time.time()))[0:9]
        filepath = os.path.join(self._root, "queue", filename[0:7], filename)

        create_if_absent(os.path.join(self._root, "queue", filename[0:7]))

        if os.path.exists(filepath):
            with open(filepath, 'r') as file:
                existing = json.load(file)
                batch = batch | existing

        with open(filepath, 'w') as file:
            json.dump(batch, file, indent=4)

    def pop_batch(self) -> Optional[Dict[str, str]]:
        def list(path):
            with os.scandir(os.path.join(path, "queue")) as entries:
                for entry in entries:
                    if entry.is_file():
                        with open(entry.path, 'r') as file:
                            content = json.load(file)
                            os.remove(entry.path)
                            return content
                    if entry.is_dir():
                        if result := list(entry.path):
                            return result

                        os.rmdir(entry.path)

            return None
   
        return list(self._root)
