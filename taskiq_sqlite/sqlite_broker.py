"""SQLite broker implementation for taskiq."""

import asyncio
import re
import time
from collections.abc import AsyncGenerator, Callable
from logging import getLogger
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

import aiosqlite
from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.sqlite_broker")

# Pattern for validating queue names to prevent SQL injection
QUEUE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_queue_name(queue_name: str) -> None:
    """
    Validate queue name to prevent SQL injection.

    :param queue_name: name to validate.
    :raises ValueError: if queue name is invalid.
    """
    if not QUEUE_NAME_PATTERN.match(queue_name):
        msg = (
            f"Invalid queue name: {queue_name!r}. "
            "Queue names must contain only alphanumeric characters and underscores."
        )
        raise ValueError(msg)


class SQLiteBroker(AsyncBroker):
    """Broker that uses SQLite as a message queue."""

    def __init__(
        self,
        db_path: str | Path = "taskiq.db",
        task_id_generator: Callable[[], str] | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        queue_name: str = "taskiq",
        poll_interval: float = 0.5,
        busy_timeout_ms: int = 5000,
        lock_timeout: float = 3600.0,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new SQLite broker.

        :param db_path: path to the SQLite database file.
        :param task_id_generator: custom task_id generator.
        :param result_backend: custom result backend.
        :param queue_name: name for the queue table in SQLite.
        :param poll_interval: interval in seconds for polling new messages.
        :param busy_timeout_ms: busy timeout for SQLite connections in milliseconds.
        :param kwargs: additional arguments.
        :raises ValueError: if queue name contains invalid characters.
        :raises RuntimeError: if database connection cannot be established.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        # Validate queue name to prevent SQL injection
        _validate_queue_name(queue_name)
        self.db_path = Path(db_path)
        self.queue_name = queue_name
        self.poll_interval = poll_interval
        self.busy_timeout_ms = busy_timeout_ms
        self.lock_timeout = lock_timeout
        self._connection: aiosqlite.Connection | None = None
        self._db_lock = asyncio.Lock()
        self._worker_id = uuid4().hex
        self._listen_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()

    async def startup(self) -> None:
        """Initialize the database connection and create tables."""
        await super().startup()
        await self._connect()
        await self._ensure_schema()

    async def shutdown(self) -> None:
        """Close the database connection."""
        self._shutdown_event.set()
        await super().shutdown()
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def kick(self, message: BrokerMessage) -> None:
        """
        Send a message to the SQLite queue.

        :param message: message to send.
        :raises RuntimeError: if database connection is not initialized.
        :raises ValueError: if queue name contains invalid characters.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        queue_name = message.labels.get("queue_name") or self.queue_name
        # Validate queue name to prevent SQL injection
        _validate_queue_name(queue_name)

        async with self._db_lock:
            # Create table for dynamic queue if needed
            if queue_name != self.queue_name:
                await self._connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {queue_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id TEXT NOT NULL,
                        message BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        processed_at REAL,
                        locked_at REAL,
                        locked_by TEXT
                    )
                    """,
                )
                await self._connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{queue_name}_status "
                    f"ON {queue_name}(status, id)",
                )
                await self._connection.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_locked_at "
                    f"ON {queue_name}(locked_at)",
                )

            await self._connection.execute(
                (f"INSERT INTO {queue_name} (task_id, message, created_at) "  # noqa: S608
                 "VALUES (?, ?, ?)"),
                (message.task_id, message.message, time.time()),
            )
            await self._connection.commit()

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """
        Listen for new messages in the SQLite queue.

        This function continuously polls the database for new messages
        and yields them with acknowledgment support.

        :yields: ackable broker messages.
        :raises RuntimeError: if database connection is not initialized.
        """
        await self._connect()
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        while not self._shutdown_event.is_set():
            row = await self._claim_next()
            if row is None:
                await asyncio.sleep(self.poll_interval)
                continue
            message_id, message_data = row

            async def ack(msg_id: int = message_id) -> None:
                await self._ack_message(msg_id)

            yield AckableMessage(
                data=message_data,
                ack=ack,
            )

    async def _connect(self) -> None:
        if self._connection is not None:
            return
        self._connection = await aiosqlite.connect(str(self.db_path))
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        await self._connection.execute("PRAGMA journal_mode = WAL")
        await self._connection.execute("PRAGMA synchronous = NORMAL")

    async def _ensure_schema(self) -> None:
        if self._connection is None:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)
        async with self._db_lock:
            await self._connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.queue_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    message BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    processed_at REAL,
                    locked_at REAL,
                    locked_by TEXT
                )
                """,
            )
            await self._connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status "
                f"ON {self.queue_name}(status, id)",
            )
            await self._connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_locked_at "
                f"ON {self.queue_name}(locked_at)",
            )
            await self._connection.commit()

    async def _claim_next(self) -> tuple[int, bytes] | None:
        if self._connection is None:
            return None
        now = time.time()
        lock_deadline = now - self.lock_timeout
        async with self._db_lock:
            await self._connection.execute("BEGIN IMMEDIATE")
            cursor = await self._connection.execute(
                (
                    f"SELECT id, message FROM {self.queue_name} "  # noqa: S608
                    "WHERE status = 'pending' AND "
                    "(locked_at IS NULL OR locked_at <= ?) "
                    "ORDER BY id LIMIT 1"
                ),
                (lock_deadline,),
            )
            row = await cursor.fetchone()
            if row is None:
                await self._connection.execute("COMMIT")
                return None
            await self._connection.execute(
                (
                    f"UPDATE {self.queue_name} "  # noqa: S608
                    "SET status = 'processing', locked_at = ?, locked_by = ? "
                    "WHERE id = ?"
                ),
                (now, self._worker_id, row["id"]),
            )
            await self._connection.commit()
        return int(row["id"]), row["message"]

    async def _ack_message(self, msg_id: int) -> None:
        """Acknowledge message processing."""
        if not self._connection:
            return
        async with self._db_lock:
            await self._connection.execute(
                f"UPDATE {self.queue_name} "  # noqa: S608
                "SET status = 'completed', processed_at = ?, "
                "locked_at = NULL, locked_by = NULL "
                "WHERE id = ?",
                (time.time(), msg_id),
            )
            await self._connection.commit()

    def __repr__(self) -> str:
        """Return string representation of the broker."""
        return (
            f"SQLiteBroker(db_path={self.db_path!r}, "
            f"queue_name={self.queue_name!r}, "
            f"poll_interval={self.poll_interval!r})"
        )
