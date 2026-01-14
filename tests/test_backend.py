"""Tests for SQLiteAsyncResultBackend."""

import asyncio
import time
from pathlib import Path

import pytest
from taskiq import TaskiqResult

from taskiq_sqlite import SQLiteResultBackend
from taskiq_sqlite.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)


@pytest.mark.asyncio
async def test_backend_startup_shutdown(temp_db_path: Path) -> None:
    """Test backend startup and shutdown."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(db_path=temp_db_path)
    await backend.startup()
    assert backend._connection is not None
    await backend.shutdown()
    assert backend._connection is None


@pytest.mark.asyncio
async def test_backend_set_and_get_result(temp_db_path: Path) -> None:
    """Test storing and retrieving a result."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(db_path=temp_db_path)
    await backend.startup()

    task_id = "test_task_123"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id, result)
    retrieved_result = await backend.get_result(task_id)

    assert retrieved_result.is_err == result.is_err
    assert retrieved_result.log == result.log
    assert retrieved_result.return_value == result.return_value
    assert retrieved_result.execution_time == result.execution_time

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_is_result_ready(temp_db_path: Path) -> None:
    """Test checking if result is ready."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(db_path=temp_db_path)
    await backend.startup()

    task_id = "test_task_456"

    # Result should not be ready initially
    assert await backend.is_result_ready(task_id) is False

    # Store a result
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )
    await backend.set_result(task_id, result)

    # Result should now be ready
    assert await backend.is_result_ready(task_id) is True

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_result_expiration(temp_db_path: Path) -> None:
    """Test result expiration."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(
        db_path=temp_db_path,
        result_ex_time=1,  # Expire after 1 second
    )
    await backend.startup()

    task_id = "test_task_expire"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id, result)

    # Result should be ready immediately
    assert await backend.is_result_ready(task_id) is True

    # Wait for expiration
    await asyncio.sleep(1.5)

    # Result should no longer be ready
    assert await backend.is_result_ready(task_id) is False

    # Getting the result should raise an error
    with pytest.raises(ResultIsMissingError):
        await backend.get_result(task_id)

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_keep_results_false(temp_db_path: Path) -> None:
    """Test that results are deleted when keep_results is False."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(
        db_path=temp_db_path,
        keep_results=False,
    )
    await backend.startup()

    task_id = "test_task_delete"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id, result)

    # Get result once
    await backend.get_result(task_id)

    # Result should be deleted now
    with pytest.raises(ResultIsMissingError):
        await backend.get_result(task_id)

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_keep_results_true(temp_db_path: Path) -> None:
    """Test that results are kept when keep_results is True."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(
        db_path=temp_db_path,
        keep_results=True,
    )
    await backend.startup()

    task_id = "test_task_keep"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id, result)

    # Get result multiple times
    await backend.get_result(task_id)
    retrieved_result = await backend.get_result(task_id)

    assert retrieved_result.return_value == "test_return"

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_result_missing(temp_db_path: Path) -> None:
    """Test retrieving a non-existent result."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(db_path=temp_db_path)
    await backend.startup()

    with pytest.raises(ResultIsMissingError):
        await backend.get_result("non_existent_task")

    await backend.shutdown()


def test_backend_invalid_expire_time_zero() -> None:
    """Test that zero expire time raises an error."""
    with pytest.raises(ExpireTimeMustBeMoreThanZeroError):
        SQLiteResultBackend(result_ex_time=0)

    with pytest.raises(ExpireTimeMustBeMoreThanZeroError):
        SQLiteResultBackend(result_px_time=0)


def test_backend_invalid_expire_time_negative() -> None:
    """Test that negative expire time raises an error."""
    with pytest.raises(ExpireTimeMustBeMoreThanZeroError):
        SQLiteResultBackend(result_ex_time=-1)

    with pytest.raises(ExpireTimeMustBeMoreThanZeroError):
        SQLiteResultBackend(result_px_time=-1)


def test_backend_duplicate_expire_time() -> None:
    """Test that setting both expire times raises an error."""
    with pytest.raises(DuplicateExpireTimeSelectedError):
        SQLiteResultBackend(result_ex_time=10, result_px_time=10000)


@pytest.mark.asyncio
async def test_backend_result_px_time(temp_db_path: Path) -> None:
    """Test result expiration using milliseconds."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(
        db_path=temp_db_path,
        result_px_time=500,  # Expire after 500ms
    )
    await backend.startup()

    task_id = "test_task_px_expire"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id, result)

    # Result should be ready immediately
    assert await backend.is_result_ready(task_id) is True

    # Wait for expiration
    await asyncio.sleep(0.7)

    # Result should no longer be ready
    assert await backend.is_result_ready(task_id) is False

    await backend.shutdown()


@pytest.mark.asyncio
async def test_backend_cleanup_expired(temp_db_path: Path) -> None:
    """Test cleanup_expired deletes expired rows."""
    backend: SQLiteResultBackend[str] = SQLiteResultBackend(
        db_path=temp_db_path,
        result_ex_time=10,
    )
    await backend.startup()

    task_id_expired = "expired_task"
    task_id_active = "active_task"
    result = TaskiqResult(
        is_err=False,
        log="Test log",
        return_value="test_return",
        execution_time=1.5,
    )

    await backend.set_result(task_id_expired, result)
    await backend.set_result(task_id_active, result)

    now = time.time()
    await backend._connection.execute(  # type: ignore[union-attr]
        f"UPDATE {backend.table_name} SET expires_at = ? WHERE task_id = ?",  # noqa: S608
        (now - 20, task_id_expired),
    )
    await backend._connection.execute(  # type: ignore[union-attr]
        f"UPDATE {backend.table_name} SET expires_at = ? WHERE task_id = ?",  # noqa: S608
        (now + 20, task_id_active),
    )
    await backend._connection.commit()  # type: ignore[union-attr]

    deleted = await backend.cleanup_expired()
    assert deleted == 1

    async with backend._connection.execute(  # type: ignore[union-attr]
        f"SELECT COUNT(*) FROM {backend.table_name}",  # noqa: S608
    ) as cursor:
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    await backend.shutdown()
