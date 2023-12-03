from loguru import logger
import psycopg
from psycopg import Connection
import tomli
from typing import Dict, Optional, Generator, List, TypedDict
from pydantic import BaseModel
from pathlib import Path
from pydantic import ValidationError
import os
import jsonlines
import click
import tqdm
from models.posts import PostEntry, PostRaw, PostMediaVariantEntry, PostFileEntry
from models.tags import TagEntry
from models.tag_alias import TagAliasEntry
from models.artists import ArtistEntry


class DatabaseConfig(BaseModel):
    dbname: str
    user: str
    password: Optional[str]


class RawDataFileNameConfig(BaseModel):
    posts: str = "posts.json"
    tags: str = "tags.json"
    artists: str = "artists.json"
    tag_aliases: str = "tag_aliases.json"
    tag_implications: str = "tag_implications.json"


class Config(BaseModel):
    database: DatabaseConfig
    file_names: RawDataFileNameConfig


def to_kv_str(d: Dict[str, str]) -> str:
    """Convert dictionary to key-value string"""
    return " ".join(f"{k}={v}" for k, v in d.items())


def postgres_env_password() -> Optional[str]:
    """Get password from environment variable"""
    return os.environ.get("PGPASSWORD")


def read_objs(path: str | Path) -> Generator[Dict[str, any], None, None]:
    """Read objects from file"""
    with jsonlines.open(path) as reader:
        for obj in reader:
            yield obj


def get_line_count(path: str | Path) -> int:
    """Get line count of file"""
    counter = 0
    with open(path, "r") as f:
        for _ in f:
            counter += 1
    return counter


def insert_post(conn: Connection, post: PostRaw) -> None:
    """
    Insert posts into database
    """
    entry = PostEntry.from_raw(post)
    columns = entry.keys()
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO booru.posts ({','.join(columns)}) VALUES ({placeholders})"

    with conn.cursor() as c:
        c.execute(sql, list(entry.values()))
    conn.commit()

    meida_variants = PostMediaVariantEntry.from_raw(post)
    for variant in meida_variants:
        columns = variant.keys()
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO booru.posts_media_variants ({','.join(columns)}) VALUES ({placeholders})"
        with conn.cursor() as c:
            c.execute(sql, list(variant.values()))
        conn.commit()

    file_entry = PostFileEntry.from_raw(post)
    columns = file_entry.keys()
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO booru.posts_file_urls ({','.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as c:
        c.execute(sql, list(file_entry.values()))
    conn.commit()


def split_tags(tags: str) -> list[str]:
    """Split tags"""
    return tags.split(" ")


def lookup_tags(conn: Connection, tags: List[str]) -> Dict[str, int]:
    """Lookup multiple tags"""
    placeholders = ', '.join(['%s'] * len(tags))

    with conn.cursor() as c:
        c.execute(f"SELECT id, name FROM booru.tags WHERE name IN ({placeholders})", tags)
        rows = c.fetchall()
        return {row[1]: row[0] for row in rows}


def associate_tags(conn: Connection, post: PostRaw) -> None:
    """Associate tags with posts"""
    tags = split_tags(post["tag_string"])
    tag_entries = lookup_tags(conn, tags)
    values = [(post["id"], tag) for tag in tag_entries]
    with conn.cursor() as c:
        c.executemany("INSERT INTO booru.posts_tags_assoc (post_id, tag_id) VALUES (%s, %s)",
                      values)
        conn.commit()


def insert_tag(conn: Connection, tag: TagEntry) -> None:
    """Insert tags into database"""
    tag = tag.model_dump()
    columns = tag.keys()
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO booru.tags ({','.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as c:
        c.execute(sql, list(tag.values()))
        conn.commit()


def insert_tag_alias(conn: Connection, tag_alias: TagAliasEntry, implication: bool = False) -> None:
    """Insert tag aliases into database"""
    tag_alias = tag_alias.model_dump()
    columns = tag_alias.keys()
    placeholders = ",".join(["%s"] * len(columns))
    if not implication:
        sql = f"INSERT INTO booru.tags_aliases ({','.join(columns)}) VALUES ({placeholders})"
    else:
        sql = f"INSERT INTO booru.tags_implications ({','.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as c:
        c.execute(sql, list(tag_alias.values()))
        conn.commit()


def insert_artist(conn: Connection, artist: ArtistEntry, other_names: list[str]) -> None:
    """Insert artists into database"""
    artist = artist.model_dump()
    columns = artist.keys()
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO booru.artists ({','.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as c:
        c.execute(sql, list(artist.values()))
        for name in other_names:
            c.execute(
                """
            INSERT INTO booru.artists_aliases (artist_id, alias) VALUES (%s, %s)
            """, (artist["id"], name))
        conn.commit()


class ContextObject(TypedDict):
    config: Config
    conn: Connection
    input_dir: Path


class Context(TypedDict):
    obj: ContextObject


@click.group()
@click.option("--config",
              "-c",
              default="config.toml",
              help="Path to config file",
              type=click.Path(exists=True))
@click.option("--input",
              "-i",
              default="raw",
              help="Path to raw data directory",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx: click.Context, config: str, input: str):
    config_dict = {}
    ctx.ensure_object(dict)
    with open(Path(config), "rb") as f:
        config_dict = tomli.load(f)
    config = Config(**config_dict)
    if not config.database.password:
        config.database.password = postgres_env_password()
    ctx.obj["config"] = config
    conn_info = to_kv_str(config.database.model_dump())
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    logger.info("Connecting to database")
    conn = psycopg.connect(conninfo=conn_info)
    ctx.obj["conn"] = conn
    p = Path(input)
    ctx.obj["input_dir"] = p


@cli.result_callback()
@click.pass_context
def close_connection(ctx, *args, **kwargs):
    logger.info("Closing connection")
    conn: Connection = ctx.obj["conn"]
    conn.close()


@cli.command()
@click.pass_context
def posts(ctx: click.Context):
    """Dump posts"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.posts
    count = get_line_count(file)
    logger.info("Dumping {} posts".format(count))
    with tqdm.tqdm(total=count, desc="Inserting posts") as pbar:
        for post in read_objs(file):
            insert_post(conn, post)
            pbar.update()


@cli.command()
@click.pass_context
def tags(ctx: click.Context):
    """Dump tags"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.tags
    count = get_line_count(file)
    logger.info("Dumping {} tags".format(count))
    with tqdm.tqdm(total=count, desc="tags") as pbar:
        for tag in read_objs(file):
            insert_tag(conn, TagEntry.from_raw(tag))
            pbar.update()


@cli.command("tag-alias")
@click.pass_context
def tag_alias(ctx: click.Context):
    """Dump tag aliases"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.tag_aliases
    count = get_line_count()
    logger.info("Dumping {} tag aliases".format(count))
    with tqdm.tqdm(total=count, desc="tag aliases") as pbar:
        for tag_alias in read_objs(file):
            insert_tag_alias(conn, TagAliasEntry.from_raw(tag_alias))
            pbar.update()


@cli.command("tag-implications")
@click.pass_context
def tag_implications(ctx: click.Context):
    """Dump tag implications"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.tag_implications
    count = get_line_count(file)
    logger.info("Dumping {} tag implications".format(count))
    with tqdm.tqdm(total=count, desc="tag implications") as pbar:
        for tag_alias in read_objs(file):
            insert_tag_alias(conn, TagAliasEntry.from_raw(tag_alias), True)
            pbar.update()


@cli.command()
@click.pass_context
def artists(ctx: click.Context):
    """Dump artists"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.artists
    count = get_line_count(file)
    logger.info("Dumping {} artists".format(count))
    with tqdm.tqdm(total=count, desc="artists") as pbar:
        for artist in read_objs(file):
            try:
                insert_artist(conn, ArtistEntry.from_raw(artist), artist["other_names"])
            except ValidationError as e:
                logger.error(e)
            pbar.update()


@cli.command("assoc-posts-tags")
@click.pass_context
def assoc_posts_tags(ctx: click.Context):
    """Associate posts with tags"""
    conn: Connection = ctx.obj["conn"]
    input_dir: Path = ctx.obj["input_dir"]
    config: Config = ctx.obj["config"]
    file = input_dir / config.file_names.posts
    count = get_line_count(file)
    logger.info("Associating {} posts with tags".format(count))
    with tqdm.tqdm(total=count, desc="Associating posts with tags") as pbar:
        for post in read_objs(file):
            associate_tags(conn, post)
            pbar.update()


if __name__ == "__main__":
    cli()
