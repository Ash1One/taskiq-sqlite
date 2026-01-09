"""SQLite broker implementation for taskiq."""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from logging import getLogger
from pathlib import Path
from typing import Any, TypeVar

import aiosqlite
from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

_T = TypeVar("_T")

logger = getLogger("taskiq.sqlite_broker")


class SQLiteBroker(AsyncBroker):
    """Broker that uses SQLite as a message queue."""

    def __init__(
        self,
        db_path: str | Path = "taskiq.db",
        task_id_generator: Callable[[], str] | None = None,
        result_backend: AsyncResultBackend[_T] | None = None,
        queue_name: str = "taskiq",
        poll_interval: float = 0.1,
        **kwargs: Any,
    ) -> None:
        """
        Construct a new SQLite broker.

        :param db_path: path to the SQLite database file.
        :param task_id_generator: custom task_id generator.
        :param result_backend: custom result backend.
        :param queue_name: name for the queue table in SQLite.
        :param poll_interval: interval in seconds for polling new messages.
        :param kwargs: additional arguments.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.db_path = Path(db_path)
        self.queue_name = queue_name
        self.poll_interval = poll_interval
        self._connection: aiosqlite.Connection | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()

    async def startup(self) -> None:
        """Initialize the database connection and create tables."""
        await super().startup()
        self._connection = await aiosqlite.connect(str(self.db_path))
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.queue_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message BLOB NOT NULL,
                created_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                processed_at REAL
            )
            """,
        )
        await self._connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status "
            f"ON {self.queue_name}(status, id)",
        )
        await self._connection.commit()

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
        """
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        queue_name = message.labels.get("queue_name") or self.queue_name
        created_at = time.time()

        # Create table for dynamic queue if needed
        if queue_name != self.queue_name:
            await self._connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {queue_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    processed_at REAL
                )
                """,
            )
            await self._connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{queue_name}_status "
                f"ON {queue_name}(status, id)",
            )

        await self._connection.execute(
            f"INSERT INTO {queue_name} (message, created_at) VALUES (?, ?)",
            (message.message, created_at),
        )
        await self._connection.commit()

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """
        Listen for new messages in the SQLite queue.

        This function continuously polls the database for new messages
        and yields them with acknowledgment support.

        :yields: ackable broker messages.
        """
        if not self._connection:
            msg = "Database connection is not initialized"
            raise RuntimeError(msg)

        while not self._shutdown_event.is_set():
            try:
                # Fetch next pending message
                async with self._connection.execute(
                    f"""
                    SELECT id, message FROM {self.queue_name}
                    WHERE status = 'pending'
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                ) as cursor:
                    row = await cursor.fetchone()

                if row:
                    message_id, message_data = row

                    # Mark as processing
                    await self._connection.execute(
                        f"UPDATE {self.queue_name} SET status = 'processing' "
                        "WHERE id = ?",
                        (message_id,),
                    )
                    await self._connection.commit()

                    async def ack() -> None:
                        """Acknowledge message processing."""
                        if not self._connection:
                            return
                        await self._connection.execute(
                            f"UPDATE {self.queue_name} "
                            "SET status = 'completed', processed_at = ? "
                            "WHERE id = ?",
                            (time.time(), message_id),
                        )
                        await self._connection.commit()

                    yield AckableMessage(
                        data=message_data,
                        ack=ack,
                    )
                else:
                    # No messages, wait before polling again
                    await asyncio.sleep(self.poll_interval)

            except Exception as exc:
                logger.exception("Error while listening for messages: %s", exc)
                await asyncio.sleep(self.poll_interval)
