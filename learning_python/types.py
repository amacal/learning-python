from typing import Any
from typing import Dict
from typing import Tuple
from typing import Iterator
from typing import Iterable
from typing import Protocol


class DataType(Protocol):
    def statistics(self, values: Iterable[Any]) -> Dict[str, str]:
        ...

    def encode(self, values: Iterable[Any]) -> Tuple[bytes, Dict[str, str]]:
        ...

    def decode(self, data: bytes, statistics: Dict[str, str]) -> Iterator[Any]:
        ...


class StringType:
    def statistics(self, values: Iterable[Any]) -> Dict[str, str]:
        nulls = sum((0 if value else 1 for value in values))
        count = sum((1 if value else 0 for value in values))

        return {
            "type": "string",
            "count": str(count),
            "nulls": str(nulls),
            "rows": str(count + nulls),
        }

    def encode(self, values: Iterable[Any]) -> Tuple[bytes, Dict[str, str]]:
        statistics = self.statistics(values)
        combined = "\n".join((str(value) or "\0" for value in values))

        return (combined.encode("utf-8"), statistics)

    def decode(self, data: bytes, statistics: Dict[str, str]) -> Iterator[Any]:
        for line in data.decode("utf-8").splitlines():
            yield line


class IntegerType:
    def __init__(self, width: int):
        self._width = width

    def statistics(self, values: Iterable[Any]) -> Dict[str, str]:
        min_value = min(values)
        max_value = max(values)

        nulls = 0
        count = sum((1 if value else 0 for value in values))

        return {
            "type": f"integer+{self._width}",
            "count": str(count),
            "nulls": str(nulls),
            "rows": str(count + nulls),
            "min": str(min_value),
            "max": str(max_value),
        }

    def encode(self, values: Iterable[Any]) -> Tuple[bytes, Dict[str, str]]:
        def encode(value: Any) -> bytes:
            return int(value).to_bytes(self._width, "big")

        statistics = self.statistics(values)
        combined = b"".join((encode(value) for value in values))

        return (combined, statistics)

    def decode(self, data: bytes, statistics: Dict[str, str]) -> Iterator[Any]:
        for chunk in (data[i : i + self._width] for i in range(0, len(data), self._width)):
            yield int.from_bytes(chunk, "big")


def get_datatype(name: str) -> DataType:
    if name == "string":
        return StringType()

    if name.startswith("integer+"):
        return IntegerType(int(name.split("+")[1]))

    raise ValueError("Unsupported data type!")


