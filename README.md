# taskiq-sqlite

Taskiq-sqlite is a plugin for taskiq that adds a new broker and result backend based on SQLite.

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq
```

This project can be installed using pip:

```bash
pip install taskiq-sqlite
```

## Usage

Let's see the example with the SQLite broker and SQLite async result backend:

```python
# broker.py
import asyncio
from pathlib import Path

from taskiq_sqlite import SQLiteAsyncResultBackend, SQLiteBroker

result_backend = SQLiteAsyncResultBackend(
    db_path="taskiq_results.db",
    keep_results=True,
    result_ex_time=3600,  # Results expire after 1 hour
)

broker = SQLiteBroker(
    db_path="taskiq.db",
    queue_name="taskiq",
).with_result_backend(result_backend)


@broker.task
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(5.5)
    print("All problems are solved!")


async def main():
    task = await best_task_ever.kiq()
    result = await task.wait_result()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

Launch the workers:

```bash
taskiq worker broker:broker
```

Then run the main code:

```bash
python3 broker.py
```

## Features

### SQLiteBroker

The SQLite broker uses a SQLite database to store task messages. It provides:

- **Persistent queue**: Messages are stored in a SQLite database and survive process restarts.
- **Message acknowledgment**: Support for message acknowledgment to ensure reliable processing.
- **Dynamic queues**: Support for multiple queues in the same database.
- **Configurable polling**: Adjust the polling interval for checking new messages.

#### Configuration

SQLiteBroker parameters:

- `db_path` - path to the SQLite database file (default: "taskiq.db")
- `task_id_generator` - custom task_id generator
- `result_backend` - custom result backend
- `queue_name` - name for the queue table (default: "taskiq")
- `poll_interval` - interval in seconds for polling new messages (default: 0.1)

### SQLiteAsyncResultBackend

The SQLite result backend stores task results in a SQLite database. It provides:

- **Persistent storage**: Results are stored in a SQLite database.
- **Result expiration**: Support for automatic result expiration.
- **Configurable retention**: Choose whether to keep results after reading.

#### Configuration

SQLiteAsyncResultBackend parameters:

- `db_path` - path to the SQLite database file (default: "taskiq_results.db")
- `keep_results` - flag to not remove results after reading (default: True)
- `result_ex_time` - expire time in seconds (default: None)
- `result_px_time` - expire time in milliseconds (default: None)
- `serializer` - custom serializer for results (default: PickleSerializer)
- `table_name` - name for the results table (default: "taskiq_results")

**Note**: Either `result_ex_time` or `result_px_time` can be set, but not both.

## Example with expiration

```python
from taskiq_sqlite import SQLiteAsyncResultBackend, SQLiteBroker

# Results expire after 1 hour
result_backend = SQLiteAsyncResultBackend(
    db_path="taskiq_results.db",
    result_ex_time=3600,
)

broker = SQLiteBroker(
    db_path="taskiq.db",
).with_result_backend(result_backend)
```

## Use Cases

SQLite-based broker and result backend are best suited for:

- **Development and testing**: Simple setup without external dependencies.
- **Small-scale applications**: Applications with low to moderate task volumes.
- **Single-node deployments**: SQLite works best on a single machine.
- **Embedded systems**: Where you want task queuing without external services.

**Warning**: SQLite is not recommended for high-throughput, distributed systems. For production use with multiple workers or high concurrency, consider using Redis, RabbitMQ, or other message brokers designed for distributed systems.
