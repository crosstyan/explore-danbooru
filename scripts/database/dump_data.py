from loguru import logger
import psycopg
from psycopg import Connection
import tomli
from typing import Dict, Optional, Generator
from pydantic import BaseModel
from pathlib import Path
import os
import jsonlines
import click
from models.posts import PostEntry


class Config(BaseModel):
    dbname: str
    user: str
    password: Optional[str]


def to_kv_str(d: Dict[str, str]) -> str:
    """Convert dictionary to key-value string"""
    return " ".join(f"{k}={v}" for k, v in d.items())


def postgres_env_password() -> Optional[str]:
    """Get password from environment variable"""
    return os.environ.get("PGPASSWORD")


def read_objs(path: str) -> Generator[Dict[str, any], None, None]:
    """Read objects from file"""
    with jsonlines.open(path) as reader:
        for obj in reader:
            yield obj


def insert_posts(conn: Connection, posts: PostEntry) -> None:
    """Insert posts into database"""
    columns = posts.keys()
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO booru.posts ({','.join(columns)}) VALUES ({placeholders})"

    with conn.cursor() as c:
        c.execute(sql, list(posts.values()))
    conn.commit()


@click.command()
@click.option("--config",
              "-c",
              default="config.toml",
              help="Path to config file",
              type=click.Path(exists=True))
@click.option("--posts",
              "-p",
              default="posts.json",
              help="Path to posts file",
              type=click.Path(exists=True))
@click.option("--tags",
              "-t",
              default="tags.json",
              help="Path to tags file",
              type=click.Path(exists=True))
def main(config: str, posts: str, tags: str):
    config_dict = {}
    with open(Path(config), "rb") as f:
        config_dict = tomli.load(f)["database"]
    config = Config(**config_dict)
    config.password = config.password if config.password else postgres_env_password()
    conn_info = to_kv_str(config.model_dump())
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    with psycopg.connect(conninfo=conn_info) as conn:
        c = conn.cursor()
        logger.info("connected")


if __name__ == "__main__":
    main()
