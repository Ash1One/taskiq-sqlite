"""Example usage of taskiq-sqlite."""

import asyncio

from taskiq_sqlite import SQLiteBroker, SQLiteResultBackend

# Create result backend with 1 hour expiration
result_backend: SQLiteResultBackend[int] = SQLiteResultBackend(
    db_path="example_results.db",
    keep_results=True,
    result_ex_time=3600,
)

# Create broker with result backend
broker = SQLiteBroker(
    db_path="example.db",
    queue_name="taskiq",
).with_result_backend(result_backend)


@broker.task
async def add_numbers(a: int, b: int) -> int:
    """Add two numbers."""
    await asyncio.sleep(0.1)  # Simulate some work
    return a + b


@broker.task
async def multiply_numbers(a: int, b: int) -> int:
    """Multiply two numbers."""
    await asyncio.sleep(0.1)  # Simulate some work
    return a * b


async def main() -> None:
    """Run example tasks."""
    # Start the broker
    await broker.startup()

    print("Kicking tasks...")

    # Kick some tasks
    task1 = await add_numbers.kiq(2, 3)
    task2 = await multiply_numbers.kiq(4, 5)
    task3 = await add_numbers.kiq(10, 20)

    print(f"Task 1 ID: {task1.task_id}")
    print(f"Task 2 ID: {task2.task_id}")
    print(f"Task 3 ID: {task3.task_id}")

    print("\nNote: To process these tasks, run:")
    print("  taskiq worker example:broker")
    print("\nOr run this script in worker mode by uncommenting the worker code below")

    # Clean up
    await broker.shutdown()


async def worker_example() -> None:
    """Example of running a worker to process tasks."""
    await broker.startup()

    print("Worker listening for tasks...")
    processed = 0

    async for message in broker.listen():
        print(f"Processing message...")
        # In real usage, taskiq worker handles this automatically
        if hasattr(message, "ack"):
            await message.ack() # type: ignore
        processed += 1
        if processed >= 3:  # Process 3 messages then stop
            break

    print(f"Processed {processed} messages")
    await broker.shutdown()


if __name__ == "__main__":
    # Run the main example
    asyncio.run(main())

    # Uncomment to run worker example:
    # asyncio.run(worker_example())
