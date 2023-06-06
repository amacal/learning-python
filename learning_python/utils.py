import os
import os.path

from typing import Dict
from typing import Tuple
from typing import TypeVar
from typing import Callable
from typing import Iterator
from typing import Iterable


def create_if_absent(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def list_directory(path: str) -> Iterator[str]:
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                yield entry.path


def read_lines(path: str) -> Iterator[str]:
    with open(path, "r") as file:
        yield from (line.strip("\n") for line in file)


def read_binary(path: str) -> bytes:
    with open(path, "rb") as file:
        return file.read()


def write_lines(path: str, lines: Iterable[str]) -> None:
    with open(path, "w") as file:
        file.write("\n".join(lines))


def write_binary(path: str, data: bytes) -> str:
    with open(path, "wb") as file:
        file.write(data)
        return path


def split_path(path: str) -> Tuple[str, Dict[str, str]]:
    key, statistics = path.split("&", maxsplit=1)
    pairs = [item.split("=") for item in statistics.split("&")]

    return (key, {key: value for key, value in pairs})


def combine_path(item: str, *args, **kwargs) -> str:
    from_args = "&".join([f"{key}={value}" for key, value in zip(args[::2], args[1::2])])
    from_kwargs = "&".join([f"{key}={value}" for key, value in sorted(kwargs.items())])

    if from_args:
        from_args = "&" + from_args

    if from_kwargs:
        from_kwargs = "&" + from_kwargs

    return f"{item}{from_args}{from_kwargs}"


T = TypeVar("T")


def as_iterable(generator: Callable[[], Iterator[T]]) -> Iterable[T]:
    class Implementation:
        def __init__(self, generator: Callable[[], Iterator[T]]):
            self._generator = generator

        def __iter__(self) -> Iterator[T]:
            return self._generator()

    return Implementation(generator)
