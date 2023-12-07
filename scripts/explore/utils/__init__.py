from loguru import logger
from psycopg import Connection
from psycopg.sql import SQL
from typing import Dict, Optional, Generator, List, TypedDict, TypeVar, Iterable, Callable, Any, Sequence
from pydantic import BaseModel
from pathlib import Path
from pydantic import ValidationError
import toolz
import jsonlines

T = TypeVar("T")

def split_tags(tags: str) -> Iterable[str]:
    """Split tags"""
    return map(lambda x: x.strip(), tags.split(" "))


def lookup_tags(conn: Connection, tags: List[str], force_remote: bool = False) -> Dict[str, int]:
    """Lookup multiple tags"""
    if not force_remote and __all_tags_table is not None:
        return {
            tag: __all_tags_table[tag] for tag in tags if tag in __all_tags_table    # type: ignore
        }

    placeholders = ", ".join(["%s"] * len(tags))

    sql = SQL(f"SELECT id, name FROM booru.tags WHERE name IN ({placeholders})")    # type: ignore
    with conn.cursor() as c:
        c.execute(sql, tags)
        rows = c.fetchall()
        return {row[1]: row[0] for row in rows}


def read_all_tags(conn: Connection) -> None:
    """Read all tags from database"""
    global __all_tags_table
    with conn.cursor() as c:
        c.execute("SELECT id, name FROM booru.tags")
        rows = c.fetchall()
        __all_tags_table = {row[1]: row[0] for row in rows}


def read_objs(path: str | Path) -> Generator[Dict[str, Any], None, None]:
    """Read objects from file"""
    with jsonlines.open(path) as reader:
        for obj in reader:
            yield obj


def batched_read_objs(path: str | Path,
                      batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
    """Read objects from file"""
    with jsonlines.open(path) as reader:
        acc: List[Dict[str, Any]] = []
        for obj in reader:
            if len(acc) >= batch_size:
                yield acc
                acc.clear()
            acc.append(obj)
        if acc:
            yield acc


def get_line_count(path: str | Path) -> int:
    """Get line count of file"""
    counter = 0
    with open(path, "r") as f:
        for _ in f:
            counter += 1
    return counter


def concat(xs: Iterable[Iterable[T]]) -> Iterable[T]:
    """Concatenate lists"""
    return toolz.concat(xs)
