"""SQLite result backend implementation for taskiq."""

import time
from pathlib import Path
from typing import Any, TypeVar

import aiosqlite
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.serializers import PickleSerializer

from taskiq_sqlite.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)

_ReturnType = TypeVar("_ReturnType")


class SQLiteAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Async result backend based on SQLite."""

    def __init__(
        self,
        db_path: str | Path = "taskiq_results.db",
        keep_results: bool = True,
        result_ex_time: int | None = None,
        result_px_time: int | None = None,
        serializer: TaskiqSerializer | None = None,
        table_name: str = "taskiq_results",
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
        self._connection: aiosqlite.Connection | None = None

        unavailable_conditions = any(
            (
                self.result_ex_time is not None and self.result_ex_time <= 0,
                self.result_px_time is not None and self.result_px_time <= 0,
            ),
        )
        if unavailable_conditions:
            raise ExpireTimeMustBeMoreThanZeroError

        if self.result_ex_time and self.result_px_time:
            raise DuplicateExpireTimeSelectedError

    async def startup(self) -> None:
        """Initialize the database connection and create tables."""
        await super().startup()
        self._connection = await aiosqlite.connect(str(self.db_path))
        await self._connection.execute("PRAGMA journal_mode=WAL")
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

    async def shutdown(self) -> None:
        """Close the database connection."""
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
        """
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
        :raises ResultIsMissingError: if result is not found or expired.
        """
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

        serialized_result, expires_at = row

        # Check if result has expired
        if expires_at is not None and current_time > expires_at:
            await self._delete_result_if_needed(task_id)
            msg = f"Result for task {task_id} has expired"
            raise ResultIsMissingError(msg)

        # Remove result if keep_results is False
        await self._delete_result_if_needed(task_id)

        # Deserialize and return result
        result_dict = self.serializer.loadb(serialized_result)
        return model_validate(TaskiqResult[_ReturnType], result_dict)

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Check if result is ready.

        :param task_id: task's id.
        :return: True if result is ready, False otherwise.
        """
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
