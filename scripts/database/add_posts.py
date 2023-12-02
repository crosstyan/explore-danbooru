from loguru import logger
import psycopg
import tomli
from typing import Dict, List, Tuple, Optional, Literal, TypedDict
from pydantic.dataclasses import dataclass
from pydantic import BaseModel
from pathlib import Path
import os
import click


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


@click.command()
@click.option("--config",
              "-c",
              default="config.toml",
              help="Path to config file",
              type=click.Path(exists=True))
def main(config: str):
    config_dict = {}
    with open(config, "rb") as f:
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
