"""Creating and upgrading reflexr's tables.

The Alembic migrations ship inside this package and record their version in their own table
(``reflexr_alembic_version``), so they run beside an application's own migrations in the same
database.
"""

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr.sql.tables import metadata


async def migrate(engine: AsyncEngine) -> None:
    """Upgrade the database to reflexr's latest schema, creating the tables if needed.

    Run it when the application deploys or starts. It is safe to run on every start: a
    database that is already up to date is left alone.
    """
    async with engine.begin() as connection:
        await connection.run_sync(_upgrade)


async def create_schema(engine: AsyncEngine) -> None:
    """Create reflexr's tables straight from the models, for tests and prototypes.

    It skips Alembic, so the database records no schema version and cannot be upgraded with
    :func:`migrate` later. Use :func:`migrate` for databases that last.
    """
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)


def _alembic_config(connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", "reflexr.sql:migrations")
    config.attributes["connection"] = connection
    return config


def _upgrade(connection: Connection) -> None:
    command.upgrade(_alembic_config(connection), "head")
