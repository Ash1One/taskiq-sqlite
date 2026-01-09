"""Tests for SQLiteBroker."""

import asyncio
from pathlib import Path

import pytest

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
            await message.ack()
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

    # Create and send a test message
    from taskiq.message import BrokerMessage

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
            await message.ack()
        break

    # Check that message is marked as completed
    async with broker._connection.execute(  # type: ignore
        f"SELECT status FROM {broker.queue_name} WHERE id = 1",
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

    # Create and send a test message
    from taskiq.message import BrokerMessage

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
            f"SELECT status FROM {broker.queue_name} WHERE id = 1",
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == "processing"

        # Acknowledge the message
        if hasattr(message, "ack"):
            await message.ack()
        break

    # Wait a bit for the ack to complete
    await asyncio.sleep(0.1)

    # Verify that the message is now completed
    async with broker._connection.execute(  # type: ignore
        f"SELECT status FROM {broker.queue_name} WHERE id = 1",
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

    # Create multiple messages
    from taskiq.message import BrokerMessage

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
            await message.ack()
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

    # Create and send a message to a custom queue
    from taskiq.message import BrokerMessage

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
