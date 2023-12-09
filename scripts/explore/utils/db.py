from loguru import logger
import psycopg
from psycopg import Connection
from psycopg.sql import SQL
from typing import Dict, Optional, Generator, List, TypedDict, TypeVar, Iterable, Callable, Any, Sequence
from pydantic import BaseModel
from pathlib import Path
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as sp
from IPython.display import display
import os


class DatabaseConfig(BaseModel):
    dbname: str
    user: str
    password: Optional[str]


class InsertionConfig(BaseModel):
    batch_count: int = 1000


class RawDataFileNameConfig(BaseModel):
    posts: str = "posts.json"
    tags: str = "tags.json"
    artists: str = "artists.json"
    artist_urls: str = "artist_urls.json"
    tag_aliases: str = "tag_aliases.json"
    tag_implications: str = "tag_implications.json"


class Config(BaseModel):
    database: DatabaseConfig
    file_names: RawDataFileNameConfig
    insertion: InsertionConfig


def to_kv_str(d: Dict[str, str]) -> str:
    """Convert dictionary to key-value string"""
    return " ".join(f"{k}={v}" for k, v in d.items())


def postgres_env_password() -> Optional[str]:
    """Get password from environment variable"""
    return os.environ.get("PGPASSWORD")


async def get_df_by_sql(conn_info: str, sql: str) -> pl.DataFrame:
    """Get dataframe by SQL"""
    async with await psycopg.AsyncConnection.connect(conninfo=conn_info) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)    # type: ignore
            rows = await cur.fetchall()
            assert cur.description is not None
            column_names = [desc[0] for desc in cur.description]
            return pl.DataFrame(rows, schema=column_names)
