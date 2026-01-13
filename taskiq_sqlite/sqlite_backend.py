"""SQLite result backend implementation for taskiq."""

import asyncio
import contextlib
import time
from logging import getLogger
from pathlib import Path
from typing import Any, TypeVar

import aiosqlite
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.serializers import PickleSerializer

from .exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)

_ReturnType = TypeVar("_ReturnType")

logger = getLogger("taskiq.sqlite_backend")


class SQLiteResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result backend based on SQLite."""

    def __init__(
        self,
        db_path: str | Path = "taskiq_results.db",
        keep_results: bool = True,
        result_ex_time: int | None = None,
        result_px_time: int | None = None,
        serializer: TaskiqSerializer | None = None,
        table_name: str = "taskiq_results",
        busy_timeout_ms: int = 5000,
        cleanup_expired_interval: float | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new SQLite result backend.

        :param db_path: path to the SQLite database file.
        :param keep_results: flag to not remove results after reading.
        :param result_ex_time: expire time in seconds for result.
        :param result_px_time: expire time in milliseconds for result.
        :param serializer: custom serializer for results.
        :param table_name: name for the results table in SQLite.
        :param busy_timeout_ms: busy timeout for SQLite connection in milliseconds.
        :param cleanup_expired_interval: interval in seconds for periodic cleanup
            of expired results.
        :param kwargs: additional arguments.

        :raises DuplicateExpireTimeSelectedError: if result_ex_time
            and result_px_time are selected.
        :raises ExpireTimeMustBeMoreThanZeroError: if result_ex_time
            or result_px_time are equal to or less than zero.
        """
        self.db_path = Path(db_path)
        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.table_name = table_name
        self.busy_timeout_ms = busy_timeout_ms
        self.cleanup_expired_interval = cleanup_expired_interval
        self._connection: aiosqlite.Connection | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError
        if cleanup_expired_interval is not None and cleanup_expired_interval <= 0:
            msg = "cleanup_expired_interval must be greater than zero"
            raise ValueError(msg)

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError

    async def startup(self) -> None:
        """Initialize the database connection and create tables."""
        await super().startup()
        await self._connect()
        await self._ensure_schema()
        if self.cleanup_expired_interval is not None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def shutdown(self) -> None:
        """Close the database connection."""
        self._shutdown_event.set()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        await super().shutdown()

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Store result in the database.

        :param task_id: task's id.
        :param result: result to store.
        :raises RuntimeError: if database connection is not initialized.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        result_dict = model_dump(result)
        serialized_result = self.serializer.dumpb(result_dict)
        created_at = time.time()

        # Calculate expiration time
        expires_at = None
        if self.result_ex_time is not None:
            expires_at = created_at + self.result_ex_time
        elif self.result_px_time is not None:
            expires_at = created_at + (self.result_px_time / 1000.0)

        await self._connection.execute(
            f"""
            INSERT OR REPLACE INTO {self.table_name}
            (task_id, result, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,  # noqa: S608
            (task_id, serialized_result, created_at, expires_at),
        )
        await self._connection.commit()

    async def _delete_result_if_needed(self, task_id: str) -> None:
        """
        Delete result from database if keep_results is False.

        :param task_id: task's id.
        """
        if not self.keep_results and self._connection:
            await self._connection.execute(
                f"DELETE FROM {self.table_name} WHERE task_id = ?", # noqa: S608
                (task_id,),
            )
            await self._connection.commit()

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Retrieve result from the database.

        :param task_id: task's id.
        :param with_logs: whether to return logs (not implemented for SQLite).
        :return: task's result.
        :raises RuntimeError: if database connection is not initialized.
        :raises ResultIsMissingError: if result is not found or expired.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        current_time = time.time()

        async with self._connection.execute(
            f"""
            SELECT result, expires_at FROM {self.table_name}
            WHERE task_id = ?
            """,  # noqa: S608
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            msg = f"Result for task {task_id} not found"
            raise ResultIsMissingError(msg)

        serialized_result = row["result"]
        expires_at = row["expires_at"]

        # Check if result has expired
        if expires_at is not None and current_time > expires_at:
            await self._delete_result_if_needed(task_id)
            msg = f"Result for task {task_id} has expired"
            raise ResultIsMissingError(msg)

        # Remove result if keep_results is False
        await self._delete_result_if_needed(task_id)

        result_dict = self.serializer.loadb(serialized_result)
        return model_validate(TaskiqResult[_ReturnType], result_dict)  # ty:ignore[invalid-argument-type, call-non-callable, invalid-return-type]

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Check if result is ready.

        :param task_id: task's id.
        :return: True if result is ready, False otherwise.
        :raises RuntimeError: if database connection is not initialized.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        current_time = time.time()

        async with self._connection.execute(
            f"""
            SELECT expires_at FROM {self.table_name}
            WHERE task_id = ?
            """,  # noqa: S608
            (task_id,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return False

        expires_at = row[0]

        # Check if result has expired
        return not (expires_at is not None and current_time > expires_at)

    async def cleanup_expired(self) -> int:
        """
        Delete expired results from the database.

        :return: number of deleted rows.
        :raises RuntimeError: if database connection is not initialized.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)
        now = time.time()
        cursor = await self._connection.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE expires_at IS NOT NULL AND expires_at < ?
            """,  # noqa: S608
            (now,),
        )
        await self._connection.commit()
        return cursor.rowcount or 0

    async def _cleanup_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await self.cleanup_expired()
            except Exception:
                logger.exception("Failed to cleanup expired results")
            await asyncio.sleep(self.cleanup_expired_interval or 0)

    async def _connect(self) -> None:
        """Establish the database connection."""
        if self._connection is not None:
            return
        self._connection = await aiosqlite.connect(str(self.db_path))
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.execute("PRAGMA synchronous = NORMAL")

    async def _ensure_schema(self) -> None:
        if self._connection is None:
            return
        await self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                task_id TEXT PRIMARY KEY,
                result BLOB NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL
            )
            """,
        )
        await self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_expires "
            f"ON {self.table_name}(expires_at)",
        )
        await self._connection.commit()
