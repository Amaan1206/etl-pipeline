"""Shared pytest fixtures for Datawatch unit and synthetic tests."""

from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd
import pytest

from datawatch.storage.database import Database


@pytest.fixture()
def sample_baseline_df() -> pd.DataFrame:
    """Return a clean baseline DataFrame with stable reference distributions."""
    rng = np.random.default_rng(202601)
    n_rows = 1000

    return pd.DataFrame(
        {
            "age": rng.normal(loc=30.0, scale=6.0, size=n_rows),
            "salary": rng.normal(loc=50000.0, scale=7000.0, size=n_rows),
            "category": rng.choice(["A", "B", "C"], size=n_rows, p=[0.4, 0.35, 0.25]),
            "active": rng.choice([True, False], size=n_rows, p=[0.78, 0.22]),
        }
    )


@pytest.fixture()
def clean_batch_df() -> pd.DataFrame:
    """Return a clean batch DataFrame aligned with baseline behavior."""
    rng = np.random.default_rng(202602)
    n_rows = 1000

    return pd.DataFrame(
        {
            "age": rng.normal(loc=30.0, scale=6.0, size=n_rows),
            "salary": rng.normal(loc=50000.0, scale=7000.0, size=n_rows),
            "category": rng.choice(["A", "B", "C"], size=n_rows, p=[0.4, 0.35, 0.25]),
            "active": rng.choice([True, False], size=n_rows, p=[0.78, 0.22]),
        }
    )


@pytest.fixture()
def corrupted_batch_df(clean_batch_df: pd.DataFrame) -> pd.DataFrame:
    """Return a corrupted batch with distribution, null-rate, and schema drift."""
    rng = np.random.default_rng(202603)
    corrupted = clean_batch_df.copy(deep=True)

    corrupted["age"] = rng.normal(loc=65.0, scale=8.0, size=len(corrupted))

    null_count = int(len(corrupted) * 0.40)
    null_indices = rng.choice(corrupted.index.to_numpy(), size=null_count, replace=False)
    corrupted.loc[null_indices, "salary"] = np.nan

    corrupted["new_column"] = rng.integers(0, 1000, size=len(corrupted))

    return corrupted


@pytest.fixture()
def temp_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield an isolated temporary SQLite database and remove it after use."""
    db_path = tmp_path / "datawatch_test.db"
    db = Database(db_path=str(db_path))

    yield db

    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()


@pytest.fixture()
def temp_csv(tmp_path: Path, sample_baseline_df: pd.DataFrame) -> Generator[str, None, None]:
    """Write a temporary CSV built from `sample_baseline_df` and clean it up."""
    csv_path = tmp_path / "sample_baseline.csv"
    sample_baseline_df.to_csv(csv_path, index=False)

    yield str(csv_path)

    if csv_path.exists():
        csv_path.unlink()
