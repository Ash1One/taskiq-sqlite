"""Tests for SQLiteBroker."""

import asyncio
import time
from pathlib import Path

import pytest
from taskiq.message import BrokerMessage

from taskiq_sqlite import SQLiteBroker


@pytest.mark.asyncio
async def test_broker_startup_shutdown(temp_db_path: Path) -> None:
    """Test broker startup and shutdown."""
    broker = SQLiteBroker(db_path=temp_db_path)
    await broker.startup()
    assert broker._connection is not None
    await broker.shutdown()
    assert broker._connection is None


@pytest.mark.asyncio
async def test_broker_kick(temp_db_path: Path) -> None:
    """Test sending a message to the broker."""
    broker = SQLiteBroker(db_path=temp_db_path)
    await broker.startup()

    # Create a task
    @broker.task
    async def test_task() -> str:
        return "test_result"

    # Kick the task
    task = await test_task.kiq()
    assert task.task_id is not None

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_listen_and_process(temp_db_path: Path) -> None:
    """Test listening for messages and processing them."""
    broker = SQLiteBroker(db_path=temp_db_path, poll_interval=0.05)
    await broker.startup()

    # Create a task
    @broker.task
    async def test_task() -> str:
        return "test_result"

    # Kick the task
    await test_task.kiq()

    # Listen for one message with timeout
    message_received = False
    timeout = 5.0
    start_time = asyncio.get_event_loop().time()

    async for message in broker.listen():
        message_received = True
        # Acknowledge the message
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        break

        # Check timeout (though break above should prevent this)
        if asyncio.get_event_loop().time() - start_time > timeout:
            break

    assert message_received

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_message_ack(temp_db_path: Path) -> None:
    """Test message acknowledgment."""
    broker = SQLiteBroker(db_path=temp_db_path, poll_interval=0.05)
    await broker.startup()

    test_message = BrokerMessage(
        task_id="test_id",
        task_name="test_task",
        message=b"test_message",
        labels={},
    )
    await broker.kick(test_message)

    # Listen and acknowledge
    async for message in broker.listen():
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        break

    # Check that message is marked as completed
    async with broker._connection.execute(  # type: ignore
        f"SELECT status FROM {broker.queue_name} WHERE id = 1",  # noqa: S608
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "completed"

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_message_processing_status(temp_db_path: Path) -> None:
    """Test message processing status tracking."""
    broker = SQLiteBroker(db_path=temp_db_path, poll_interval=0.05)
    await broker.startup()

    test_message = BrokerMessage(
        task_id="test_id",
        task_name="test_task",
        message=b"test_message",
        labels={},
    )
    await broker.kick(test_message)

    # Listen and check that message is marked as processing
    async for message in broker.listen():
        # Check that message is marked as processing in database
        async with broker._connection.execute(  # type: ignore
            f"SELECT status FROM {broker.queue_name} WHERE id = 1",  # noqa: S608
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "processing"

        # Acknowledge the message
        if hasattr(message, "ack"):
            await message.ack()  # type: ignore
        break

    # Wait a bit for the ack to complete
    await asyncio.sleep(0.1)

    # Verify that the message is now completed
    async with broker._connection.execute(  # type: ignore
        f"SELECT status FROM {broker.queue_name} WHERE id = 1",  # noqa: S608
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "completed"

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_multiple_messages(temp_db_path: Path) -> None:
    """Test processing multiple messages."""
    broker = SQLiteBroker(db_path=temp_db_path, poll_interval=0.05)
    await broker.startup()

    for i in range(5):
        test_message = BrokerMessage(
            task_id=f"test_id_{i}",
            task_name="test_task",
            message=f"test_message_{i}".encode(),
            labels={},
        )
        await broker.kick(test_message)

    # Process all messages with timeout
    messages_processed = 0
    timeout = 5.0
    start_time = asyncio.get_event_loop().time()

    async for message in broker.listen():
        messages_processed += 1
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        if messages_processed >= 5:
            break

        # Check timeout
        if asyncio.get_event_loop().time() - start_time > timeout:
            break

    assert messages_processed == 5

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_dynamic_queue(temp_db_path: Path) -> None:
    """Test using dynamic queue names."""
    broker = SQLiteBroker(db_path=temp_db_path, poll_interval=0.05)
    await broker.startup()

    test_message = BrokerMessage(
        task_id="test_id",
        task_name="test_task",
        message=b"test_message",
        labels={"queue_name": "custom_queue"},
    )
    await broker.kick(test_message)

    # Verify the message was stored in the custom queue
    async with broker._connection.execute(  # type: ignore
        "SELECT COUNT(*) FROM custom_queue",
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_invalid_queue_name(temp_db_path: Path) -> None:
    """Test that invalid queue names are rejected."""
    broker = SQLiteBroker(db_path=temp_db_path)
    await broker.startup()

    test_message = BrokerMessage(
        task_id="test_id",
        task_name="test_task",
        message=b"test_message",
        labels={"queue_name": "queue; DROP TABLE taskiq; --"},
    )

    with pytest.raises(ValueError, match="Invalid queue name"):
        await broker.kick(test_message)

    await broker.shutdown()


def test_broker_invalid_queue_name_in_constructor(temp_db_path: Path) -> None:
    """Test that invalid queue names are rejected in constructor."""
    with pytest.raises(ValueError, match="Invalid queue name"):
        SQLiteBroker(db_path=temp_db_path, queue_name="queue; DROP TABLE --")


@pytest.mark.asyncio
async def test_broker_cleanup_completed_ttl(temp_db_path: Path) -> None:
    """Test cleanup by TTL removes old completed messages."""
    broker = SQLiteBroker(
        db_path=temp_db_path,
        poll_interval=0.01,
        cleanup_completed_ttl=10,
    )
    await broker.startup()

    for i in range(2):
        test_message = BrokerMessage(
            task_id=f"test_id_{i}",
            task_name="test_task",
            message=f"test_message_{i}".encode(),
            labels={},
        )
        await broker.kick(test_message)

    processed = 0
    async for message in broker.listen():
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        processed += 1
        if processed >= 2:
            break

    async with broker._connection.execute(  # type: ignore[union-attr]
        f"SELECT id FROM {broker.queue_name} ORDER BY id",  # noqa: S608
    ) as cursor:
        rows = await cursor.fetchall()
    old_id = rows[0][0] # type: ignore
    new_id = rows[1][0] # type: ignore
    now = time.time()
    await broker._connection.execute(  # type: ignore[union-attr]
        f"UPDATE {broker.queue_name} SET processed_at = ? WHERE id = ?",  # noqa: S608
        (now - 20, old_id),
    )
    await broker._connection.execute(  # type: ignore[union-attr]
        f"UPDATE {broker.queue_name} SET processed_at = ? WHERE id = ?",  # noqa: S608
        (now, new_id),
    )
    await broker._connection.commit()  # type: ignore[union-attr]

    deleted = await broker.cleanup_completed()
    assert deleted == 1

    async with broker._connection.execute(  # type: ignore[union-attr]
        f"SELECT COUNT(*) FROM {broker.queue_name} WHERE status = 'completed'",  # noqa: S608
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    await broker.shutdown()


@pytest.mark.asyncio
async def test_broker_cleanup_completed_max_records(temp_db_path: Path) -> None:
    """Test cleanup keeps only the most recent completed messages."""
    broker = SQLiteBroker(
        db_path=temp_db_path,
        poll_interval=0.01,
        cleanup_completed_max_records=1,
    )
    await broker.startup()

    for i in range(3):
        test_message = BrokerMessage(
            task_id=f"test_id_{i}",
            task_name="test_task",
            message=f"test_message_{i}".encode(),
            labels={},
        )
        await broker.kick(test_message)

    processed = 0
    async for message in broker.listen():
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        processed += 1
        if processed >= 3:
            break

    async with broker._connection.execute(  # type: ignore[union-attr]
        f"SELECT id FROM {broker.queue_name} ORDER BY id",  # noqa: S608
    ) as cursor:
        rows = await cursor.fetchall()

    processed_times = [100.0, 200.0, 300.0]
    for row, processed_at in zip(rows, processed_times, strict=True):
        await broker._connection.execute(  # type: ignore[union-attr]
            f"UPDATE {broker.queue_name} SET processed_at = ? WHERE id = ?",  # noqa: S608
            (processed_at, row[0]),
        )
    await broker._connection.commit()  # type: ignore[union-attr]

    deleted = await broker.cleanup_completed()
    assert deleted == 2

    async with broker._connection.execute(  # type: ignore[union-attr]
        f"SELECT id, processed_at FROM {broker.queue_name} "  # noqa: S608
        "WHERE status = 'completed'",
    ) as cursor:
        rows = list(await cursor.fetchall())
    assert len(rows) == 1
    assert rows[0]["processed_at"] == 300.0

    await broker.shutdown()
