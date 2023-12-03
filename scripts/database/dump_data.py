from loguru import logger
import psycopg
from psycopg import Connection
import tomli
from typing import Dict, Optional, Generator, List, TypedDict, TypeVar, Iterable, Callable, Any
from pydantic import BaseModel
from pathlib import Path
from pydantic import ValidationError
import os
import jsonlines
import click
import tqdm
import toolz
from models.posts import PostEntry, PostRaw, PostMediaVariantEntry, PostFileEntry
from models.tags import TagEntry
from models.tag_alias import TagAliasEntry
from models.artists import ArtistEntry
from models.artist_urls import ArtistUrlEntry

T = TypeVar("T")
U = TypeVar("U", bound=BaseModel)
V = TypeVar("V")

__all_tags_table: Optional[dict[str, int]] = None


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


def concat(xs: Iterable[Iterable[T]]) -> Iterable[T]:
    """Concatenate lists"""
    return toolz.concat(xs)


def get_id_tag_pairs(posts: Iterable[PostRaw]) -> Generator[tuple[int, str], None, None]:
    for post in posts:
        tags = split_tags(post["tag_string"])
        id_tags = ((post["id"], tag) for tag in tags)
        yield from id_tags


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

    with conn.cursor() as c:
        c.execute(f"SELECT id, name FROM booru.tags WHERE name IN ({placeholders})", tags)
        rows = c.fetchall()
        return {row[1]: row[0] for row in rows}


def read_all_tags(conn: Connection) -> None:
    """Read all tags from database"""
    global __all_tags_table
    with conn.cursor() as c:
        c.execute("SELECT id, name FROM booru.tags")
        rows = c.fetchall()
        __all_tags_table = {row[1]: row[0] for row in rows}


def read_objs(path: str | Path) -> Generator[Dict[str, any], None, None]:
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


def batched_insert_base(conn: Connection, entries: List[U], table_name: str) -> None:
    """Insert entries into database in batch"""
    if not entries:
        return

    entries_dict_list = [entry.model_dump() for entry in entries]

    columns = entries_dict_list[0].keys()
    placeholders = ",".join(["%s"] * len(columns))

    sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

    with conn.cursor() as c:
        # Prepare the list of values for executemany()
        values = [list(entry.values()) for entry in entries_dict_list]
        c.executemany(sql, values)

        conn.commit()


def batched_insert_posts(conn: Connection,
                         posts: List[PostRaw],
                         assoc_tags: bool = True,
                         fetch_all_tags: bool = True) -> None:
    """
    Insert posts into database in batch.

    assoc_tags should only be true if the tags are already in the database,
    since it depends on the tags table.
    """
    if not posts:
        return

    if fetch_all_tags:
        if __all_tags_table is None:
            # I assume the tags won't change during the insertion of posts.
            read_all_tags(conn)

    entries = [PostEntry.from_raw(post) for post in posts]
    media_variants = [PostMediaVariantEntry.from_raw(post) for post in posts]
    flatten_variants = list(concat(media_variants))
    file_entries = [PostFileEntry.from_raw(post) for post in posts]

    ids_tags: Optional[list[tuple[int, str]]] = None
    lookup_table: Optional[dict[str, int]] = None

    if assoc_tags:
        # the ideal way is using a cache instead of building a lookup table
        # every time.
        ids_tags = list(get_id_tag_pairs(posts))
        only_tags = list(map(lambda x: x[1], ids_tags))
        if __all_tags_table is None:
            lookup_table = lookup_tags(conn, only_tags)
        else:
            lookup_table = __all_tags_table

    def batch_entries():
        batched_insert_base(conn, entries, "booru.posts")

    def batch_media_variants():
        batched_insert_base(conn, flatten_variants, "booru.posts_media_variants")

    def batch_file_entries():
        batched_insert_base(conn, file_entries, "booru.posts_file_urls")

    def batch_assoc_tags(table: dict[str, int], id_tag_pairs: Iterable[tuple[int, str]]):
        with conn.cursor() as c:
            c.executemany("INSERT INTO booru.posts_tags_assoc (post_id, tag_id) VALUES (%s, %s)",
                          [(post_id, table[tag]) for post_id, tag in id_tag_pairs])
            conn.commit()

    batch_entries()
    batch_media_variants()
    batch_file_entries()
    if assoc_tags:
        assert lookup_table is not None
        assert ids_tags is not None
        batch_assoc_tags(lookup_table, ids_tags)


def batched_insert_tags(conn: Connection, tags: List[TagEntry]) -> None:
    """Insert tags into database in batch"""
    batched_insert_base(conn, tags, "booru.tags")


def batch_insert_tag_alias(conn: Connection,
                           tag_aliases: List[TagAliasEntry],
                           implication: bool = False) -> None:
    """Insert tag aliases into database"""
    if implication:
        batched_insert_base(conn, tag_aliases, "booru.tag_implications")
    else:
        batched_insert_base(conn, tag_aliases, "booru.tag_aliases")


def batched_insert_artists(conn: Connection, artists: List[ArtistEntry]) -> None:
    """Insert artists and their aliases into database in batch"""
    if not artists:
        return

    artists_dict_list = [artist.model_dump() for artist in artists]

    columns = artists_dict_list[0].keys()
    placeholders = ",".join(["%s"] * len(columns))

    sql = f"INSERT INTO booru.artists ({','.join(columns)}) VALUES ({placeholders})"

    with conn.cursor() as c:
        values = [list(artist.values()) for artist in artists_dict_list]
        c.executemany(sql, values)

        aliases = [
            (artist["id"], alias) for artist in artists_dict_list for alias in artist["other_names"]
        ]
        if aliases:
            c.executemany(
                "INSERT INTO booru.artists_aliases (artist_id, alias) VALUES (%s, %s)",
                aliases,
            )
        conn.commit()


def batched_insert_artist_urls(conn: Connection, artist_urls: List[ArtistUrlEntry]) -> None:
    """Insert artist urls into database in batch"""
    batched_insert_base(conn, artist_urls, "booru.artists_urls")


class ContextObject(TypedDict):
    config: Config
    conn: Connection
    input_dir: Path


class Context(TypedDict):
    obj: ContextObject


def create_group():

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

    @cli.command()
    @click.pass_context
    @click.argument("tag_list", type=str, nargs=-1)
    def lookup_tag(ctx: click.Context, tag_list: list[str]):
        """Lookup tag"""
        conn: Connection = ctx.obj["conn"]
        lookup_table = lookup_tags(conn, tag_list)
        logger.info(lookup_table)

    def process_data(obj: ContextObject,
                     entry: str,
                     insert_fn: Callable[[Connection, List[T]], None],
                     transform_fn: Optional[Callable[[dict[str, Any]], T]] = None) -> None:
        conn = obj["conn"]
        input_dir = obj["input_dir"]
        config = obj["config"]
        file = input_dir / getattr(config.file_names, entry)
        count = get_line_count(file)
        logger.info("Dumping {} {}".format(count, entry))
        with tqdm.tqdm(total=count, desc=entry) as pbar:
            for batched in batched_read_objs(file, config.insertion.batch_count):
                if transform_fn is None:
                    insert_fn(conn, batched)
                else:
                    insert_fn(conn, [transform_fn(item) for item in batched])
                pbar.update(len(batched))

    @cli.result_callback()
    @click.pass_context
    def close_connection(ctx, *_args, **_kwargs):
        logger.info("Closing connection")
        conn: Connection = ctx.obj["conn"]
        conn.close()

    @cli.command()
    @click.pass_context
    def posts(ctx: click.Context):
        """Dump posts"""
        process_data(ctx.obj, "posts", lambda conn, raw: batched_insert_posts(conn, raw))

    @cli.command()
    @click.pass_context
    def tags(ctx: click.Context):
        """Dump tags"""
        process_data(ctx.obj, "tags", batched_insert_tags, TagEntry.from_raw)

    @cli.command()
    @click.pass_context
    def tag_alias(ctx: click.Context):
        """Dump tag aliases"""
        process_data(ctx.obj, "tag_aliases", batch_insert_tag_alias, TagAliasEntry.from_raw)

    @cli.command()
    @click.pass_context
    def tag_implications(ctx: click.Context):
        """Dump tag implications"""
        process_data(ctx.obj, "tag_implications",
                     lambda conn, raw: batch_insert_tag_alias(conn, raw, implication=True),
                     TagAliasEntry.from_raw)

    @cli.command()
    @click.pass_context
    def artists(ctx: click.Context):
        """Dump artists"""
        process_data(ctx.obj, "artists", batched_insert_artists, ArtistEntry.from_raw)

    @cli.command()
    @click.pass_context
    def artist_urls(ctx: click.Context):
        """Dump artist urls"""
        process_data(ctx.obj, "artist_urls", batched_insert_artist_urls, ArtistUrlEntry.from_raw)

    return cli


if __name__ == "__main__":
    create_group()()
