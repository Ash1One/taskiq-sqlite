"""Configuration for pytest."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db_path() -> Path:
    """Create a temporary database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)
    yield db_path
    # Cleanup
    if db_path.exists():
        db_path.unlink()
    # Also clean up WAL and SHM files
    wal_path = db_path.with_suffix(".db-wal")
    if wal_path.exists():
        wal_path.unlink()
    shm_path = db_path.with_suffix(".db-shm")
    if shm_path.exists():
        shm_path.unlink()
