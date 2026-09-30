"""SQLite engines that serialize transactions the way :class:`~reflexr.sql.SqlStorage` needs.

SQLite has no row locks and ignores ``FOR UPDATE``. Instead, every transaction takes the
database's write lock as it begins (``BEGIN IMMEDIATE``), so a transaction's reads cannot go
stale before it writes, and concurrent writers wait for each other instead of failing.

Since transactions run one at a time anyway, an engine keeps a single connection. A process's
transactions queue for it and take turns in the order they begin, as they do on in-memory
storage, instead of polling for the write lock; only other processes contend for the lock.
"""

from typing import Any

from sqlalchemy import URL, Connection, event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool


def create_sqlite_engine(url: str | URL, **kwargs: Any) -> AsyncEngine:
    """Create an async SQLite engine for :class:`~reflexr.sql.SqlStorage`.

    Every transaction on the engine, reads included, holds the database's write lock, so
    transactions run one at a time, on the engine's one connection. A transaction waits for the
    connection for up to the engine's ``pool_timeout`` (30 seconds), and for another process's
    lock for up to the ``timeout`` connect argument of Python's ``sqlite3`` (5 seconds unless
    ``connect_args`` says otherwise). Use a database file (or a shared-cache memory database).

    Shared verbatim with artifactr's ``src/artifactr/sql/sqlite.py``; change both.

    Args:
        url: A SQLite URL for an async driver, such as ``sqlite+aiosqlite:///app.db``
            (install ``reflexr[sqlite]``).
        **kwargs: Passed to :func:`sqlalchemy.ext.asyncio.create_async_engine`. Give
            ``poolclass`` (with its own arguments) to pool connections differently.

    Returns:
        The engine. Dispose of it when you are done.
    """
    if "poolclass" not in kwargs:
        kwargs = {"poolclass": AsyncAdaptedQueuePool, "pool_size": 1, "max_overflow": 0, **kwargs}
    engine = create_async_engine(url, **kwargs)
    event.listen(engine.sync_engine, "connect", _leave_transactions_to_sqlalchemy)
    event.listen(engine.sync_engine, "begin", _begin_immediate)
    return engine


def _leave_transactions_to_sqlalchemy(dbapi_connection: Any, _record: Any) -> None:
    # sqlite3 would otherwise begin transactions itself, lazily and only before writes, which
    # also breaks savepoints and transactional DDL.
    dbapi_connection.isolation_level = None


def _begin_immediate(connection: Connection) -> None:
    connection.exec_driver_sql("BEGIN IMMEDIATE")
