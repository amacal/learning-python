import time
import pytest

from learning_python.main import _hash
from learning_python.main import _last
from learning_python.main import _first

from learning_python.main import CodeDiff
from learning_python.main import CodeFile


@pytest.fixture
def now() -> str:
    return str(int(time.time()))


def test_chunks_lines_without_collisions() -> None:
    lines = ["abc\n", "cde\n"]

    collection = CodeFile(lines).chunk()
    chunks = list(collection.iterate())

    assert len(chunks) == 2

    assert len(chunks[0].get_lines()) == 1
    assert chunks[0].get_characters() == 4

    assert chunks[0].get_lines()[0].get_index() == 0
    assert chunks[0].get_lines()[0].get_value() == "abc\n"
    assert chunks[0].get_lines()[0].get_hash() == _hash("abc\n")

    assert len(chunks[1].get_lines()) == 1
    assert chunks[1].get_characters() == 4

    assert chunks[1].get_lines()[0].get_index() == 1
    assert chunks[1].get_lines()[0].get_value() == "cde\n"
    assert chunks[1].get_lines()[0].get_hash() == _hash("cde\n")


def test_chunks_lines_with_collisions() -> None:
    lines = ["abc\n", "cde\n", "abc\n", "fgh"]

    collection = CodeFile(lines).chunk()
    chunks = list(collection.iterate())

    assert len(chunks) == 2

    assert len(chunks[0].get_lines()) == 2
    assert chunks[0].get_characters() == 8

    assert chunks[0].get_lines()[0].get_index() == 0
    assert chunks[0].get_lines()[0].get_value() == "abc\n"
    assert chunks[0].get_lines()[0].get_hash() == _hash("abc\n")
    assert chunks[0].get_lines()[1].get_index() == 1
    assert chunks[0].get_lines()[1].get_value() == "cde\n"
    assert chunks[0].get_lines()[1].get_hash() == _hash("cde\n")

    assert len(chunks[1].get_lines()) == 2
    assert chunks[1].get_characters() == 7

    assert chunks[1].get_lines()[0].get_index() == 2
    assert chunks[1].get_lines()[0].get_value() == "abc\n"
    assert chunks[1].get_lines()[0].get_hash() == _hash("abc\n")
    assert chunks[1].get_lines()[1].get_index() == 3
    assert chunks[1].get_lines()[1].get_value() == "fgh"
    assert chunks[1].get_lines()[1].get_hash() == _hash("fgh")


def test_splits_lines_into_single_chunk() -> None:
    lines = ["abc\n", "cde\n"]

    collection = CodeFile(lines).chunk()
    chunks = list(collection.split(10).iterate())

    assert len(chunks) == 1
    assert len(chunks[0].get_lines()) == 2
    assert chunks[0].get_characters() == 8

    assert chunks[0].get_lines()[0].get_index() == 0
    assert chunks[0].get_lines()[0].get_value() == "abc\n"
    assert chunks[0].get_lines()[0].get_hash() == _hash("abc\n")
    assert chunks[0].get_lines()[1].get_index() == 1
    assert chunks[0].get_lines()[1].get_value() == "cde\n"
    assert chunks[0].get_lines()[1].get_hash() == _hash("cde\n")


def test_splits_lines_into_two_chunks() -> None:
    lines = ["abcdefghijkl\n", "cdefgh\n"]

    collection = CodeFile(lines).chunk()
    chunks = list(collection.split(10).iterate())

    assert len(chunks) == 2

    assert len(chunks[0].get_lines()) == 1
    assert chunks[0].get_characters() == 13

    assert chunks[0].get_lines()[0].get_index() == 0
    assert chunks[0].get_lines()[0].get_value() == "abcdefghijkl\n"
    assert chunks[0].get_lines()[0].get_hash() == _hash("abcdefghijkl\n")

    assert len(chunks[1].get_lines()) == 1
    assert chunks[1].get_characters() == 7

    assert chunks[1].get_lines()[0].get_index() == 1
    assert chunks[1].get_lines()[0].get_value() == "cdefgh\n"
    assert chunks[1].get_lines()[0].get_hash() == _hash("cdefgh\n")


def test_refs_from_single_chunk(now: str) -> None:
    lines = ["abc\n", "cde\n"]

    collection = CodeFile(lines).chunk()
    refs = list(CodeDiff.create(collection.split(10), now).iterate())

    assert len(refs) == 1
    assert refs[0].get_start() == _first()
    assert refs[0].get_end() == _last()
    assert refs[0].get_hash() == _hash("abc\ncde\n")


def test_refs_from_multiple_chunks(now: str) -> None:
    lines = ["abcdefghijkl\n", "a\n", "cdefgh\n"]

    collection = CodeFile(lines).chunk()
    refs = list(CodeDiff.create(collection.split(10), now).iterate())

    assert len(refs) == 2

    assert refs[0].get_start() == _first()
    assert refs[0].get_end() == _hash("abcdefghijkl\n")
    assert refs[0].get_hash() == _hash("abcdefghijkl\n")

    assert refs[1].get_start() == _hash("abcdefghijkl\n")
    assert refs[1].get_end() == _last()
    assert refs[1].get_hash() == _hash("a\ncdefgh\n")


def test_diffs_from_unchanged_chunks(now: str) -> None:
    lines = ["abcdefghijkl\n", "a\n", "cdefgh\n"]

    collection = CodeFile(lines).chunk()
    diff = CodeDiff.create(collection.split(10), now)

    matched, unmatched = diff.extract(collection)
    refs = list(CodeDiff.create(matched, now).iterate())

    assert len(list(matched.iterate())) == 2
    assert len(list(unmatched.iterate())) == 0

    assert refs[0].get_start() == _first()
    assert refs[0].get_end() == _hash("abcdefghijkl\n")
    assert refs[0].get_hash() == _hash("abcdefghijkl\n")

    assert refs[1].get_start() == _hash("abcdefghijkl\n")
    assert refs[1].get_end() == _last()
    assert refs[1].get_hash() == _hash("a\ncdefgh\n")


def test_diffs_from_changed_chunks(now: str) -> None:
    before = ["abcdefghijkl\n", "a\n", "cdefgh\n"]
    after = ["abcdefghijkl\n", "b\n", "cdefgh\n"]

    collection = CodeFile(after).chunk()
    diff = CodeDiff.create(CodeFile(before).chunk().split(10), now)

    matched, unmatched = diff.extract(collection)
    unmatched = unmatched.split(10)

    matched_refs = list(CodeDiff.create(matched, now).iterate())
    unmatched_refs = list(CodeDiff.create(unmatched, now).iterate())

    assert len(matched_refs) == 1
    assert len(unmatched_refs) == 1

    assert matched_refs[0].get_start() == _first()
    assert matched_refs[0].get_end() == _hash("abcdefghijkl\n")
    assert matched_refs[0].get_hash() == _hash("abcdefghijkl\n")

    assert unmatched_refs[0].get_start() == _hash("abcdefghijkl\n")
    assert unmatched_refs[0].get_end() == _last()
    assert unmatched_refs[0].get_hash() == _hash("b\ncdefgh\n")


def test_diffs_from_changed_chunks_end_boundary(now: str) -> None:
    before = ["abcdefghijkl\n", "a\n", "cdefgh\n"]
    after = ["abcdefghijkl\n", "a\n", "-defgh\n"]

    collection = CodeFile(after).chunk()
    diff = CodeDiff.create(CodeFile(before).chunk().split(10), now)

    matched, unmatched = diff.extract(collection)
    unmatched = unmatched.split(10)

    matched_refs = list(CodeDiff.create(matched, now).iterate())
    unmatched_refs = list(CodeDiff.create(unmatched, now).iterate())

    assert len(matched_refs) == 1
    assert len(unmatched_refs) == 1

    assert matched_refs[0].get_start() == _first()
    assert matched_refs[0].get_end() == _hash("abcdefghijkl\n")
    assert matched_refs[0].get_hash() == _hash("abcdefghijkl\n")

    assert unmatched_refs[0].get_start() == _hash("abcdefghijkl\n")
    assert unmatched_refs[0].get_end() == _last()
    assert unmatched_refs[0].get_hash() == _hash("a\n-defgh\n")


def test_diffs_from_changed_chunks_start_boundary(now: str) -> None:
    before = ["abcdefghijkl\n", "a\n", "cdefgh\n"]
    after = ["abcdefghijk-\n", "a\n", "cdefgh\n"]

    collection = CodeFile(after).chunk()
    diff = CodeDiff.create(CodeFile(before).chunk().split(10), now)

    matched, unmatched = diff.extract(collection)
    unmatched = unmatched.split(10)

    matched_refs = list(CodeDiff.create(matched, now).iterate())
    unmatched_refs = list(CodeDiff.create(unmatched, now).iterate())

    assert len(matched_refs) == 0
    assert len(unmatched_refs) == 2

    assert unmatched_refs[0].get_start() == _first()
    assert unmatched_refs[0].get_end() == _hash("abcdefghijk-\n")
    assert unmatched_refs[0].get_hash() == _hash("abcdefghijk-\n")

    assert unmatched_refs[1].get_start() == _hash("abcdefghijk-\n")
    assert unmatched_refs[1].get_end() == _last()
    assert unmatched_refs[1].get_hash() == _hash("a\ncdefgh\n")


def test_diffs_from_changed_twice(now: str) -> None:
    before = ["abc\n", "1\n", "defghijkl\n", "01234567890\n", "a\n", "cdefgh\n"]
    after = ["abc\n", "2\n", "defghijkl\n", "01234567890\n", "b\n", "cdefgh\n"]

    collection = CodeFile(after).chunk()
    diff = CodeDiff.create(CodeFile(before).chunk().split(10), now)

    matched, unmatched = diff.extract(collection)
    unmatched = unmatched.split(10)

    matched_refs = list(CodeDiff.create(matched, now).iterate())
    unmatched_refs = list(CodeDiff.create(unmatched, now).iterate())

    assert len(matched_refs) == 1
    assert len(unmatched_refs) == 2

    assert matched_refs[0].get_start() == _hash("defghijkl\n")
    assert matched_refs[0].get_end() == _hash("01234567890\n")
    assert matched_refs[0].get_hash() == _hash("01234567890\n")

    assert unmatched_refs[0].get_start() == _first()
    assert unmatched_refs[0].get_end() == _hash("defghijkl\n")
    assert unmatched_refs[0].get_hash() == _hash("abc\n2\ndefghijkl\n")

    assert unmatched_refs[1].get_start() == _hash("01234567890\n")
    assert unmatched_refs[1].get_end() == _last()
    assert unmatched_refs[1].get_hash() == _hash("b\ncdefgh\n")


def test_diffs_from_disappeared_uniqueness(now: str) -> None:
    before = ["abc\n", "1\n", "defghijkl\n", "01234567890\n", "a\n", "cdefgh\n"]
    after = ["abc\n", "2\n", "defghijkl\n", "01234567890\n", "b\n", "defghijkl\n"]

    collection = CodeFile(after).chunk()
    diff = CodeDiff.create(CodeFile(before).chunk().split(10), now)

    matched, unmatched = diff.extract(collection)
    unmatched = unmatched.split(10)

    matched_refs = list(CodeDiff.create(matched, now).iterate())
    unmatched_refs = list(CodeDiff.create(unmatched, now).iterate())

    assert len(matched_refs) == 0
    assert len(unmatched_refs) == 2

    assert unmatched_refs[0].get_start() == _first()
    assert unmatched_refs[0].get_end() == _hash("01234567890\n")
    assert unmatched_refs[0].get_hash() == _hash("abc\n2\ndefghijkl\n01234567890\n")

    assert unmatched_refs[1].get_start() == _hash("01234567890\n")
    assert unmatched_refs[1].get_end() == _last()
    assert unmatched_refs[1].get_hash() == _hash("b\ndefghijkl\n")
