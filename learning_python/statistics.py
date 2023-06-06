from typing import Dict
from typing import List
from typing import Optional
from typing import Iterator
from typing import Callable

from itertools import groupby


_additive: Dict[str, Callable[[Iterator[str]], str]] = {
    "type": lambda items: items.__next__(),
    "count": lambda items: str(sum((int(item) for item in items))),
    "nulls": lambda items: str(sum((int(item) for item in items))),
    "min": lambda items: str(min((int(item) for item in items))),
    "max": lambda items: str(max((int(item) for item in items))),
    "rows": lambda items: str(sum((int(item) for item in items))),
}


class DataStatistics:
    def __init__(self, entries: Dict[str, str]):
        self._entries = entries

    @staticmethod
    def combine(statistics: List["DataStatistics"], only: Optional[List[str]] = None) -> "DataStatistics":
        def included(key: str) -> bool:
            return key in _additive and (only is None or key in only)

        entries = sorted((key, value) for item in statistics for key, value in item._entries.items())
        groups = groupby((item for item in entries if included(item[0])), key=lambda entry: entry[0])

        return DataStatistics({key: _additive[key](item[1] for item in items) for key, items in groups})

    def as_dict(self) -> Dict[str, str]:
        return self._entries
