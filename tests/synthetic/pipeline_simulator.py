"""Synthetic ETL pipeline batch generator for benchmark tests."""

from typing import List

import numpy as np
import pandas as pd

from tests.synthetic.corruption_injector import CorruptionInjector


class PipelineSimulator:
    """Generate healthy and corrupted synthetic ETL batches."""

    def __init__(self, rows_per_batch: int = 1000, seed: int = 2026) -> None:
        self.rows_per_batch = int(rows_per_batch)
        self.seed = int(seed)

    def _batch_rng(self, batch_index: int) -> np.random.Generator:
        """Return a deterministic RNG for a given batch index."""
        return np.random.default_rng(self.seed + (batch_index * 7919))

    def _generate_batch(self, batch_index: int) -> pd.DataFrame:
        """Build one healthy batch with realistic production-like columns."""
        rng = self._batch_rng(batch_index)

        mean_drift = float(rng.normal(loc=0.0, scale=0.15))

        return pd.DataFrame(
            {
                "transaction_amount": rng.normal(
                    loc=120.0 + mean_drift,
                    scale=25.0,
                    size=self.rows_per_batch,
                ),
                "user_age": rng.normal(
                    loc=34.0 + mean_drift,
                    scale=9.0,
                    size=self.rows_per_batch,
                ),
                "product_category": rng.choice(
                    ["electronics", "fashion", "grocery", "books"],
                    size=self.rows_per_batch,
                    p=[0.28, 0.24, 0.30, 0.18],
                ),
                "is_active": rng.choice(
                    [True, False],
                    size=self.rows_per_batch,
                    p=[0.82, 0.18],
                ),
                "session_duration": rng.normal(
                    loc=14.0 + (mean_drift * 0.1),
                    scale=4.5,
                    size=self.rows_per_batch,
                ),
            }
        )

    def generate_healthy_batches(self, n: int) -> List[pd.DataFrame]:
        """Return `n` healthy batches with natural variation."""
        if n <= 0:
            raise ValueError("n must be greater than 0")

        baseline = self._generate_batch(0).reset_index(drop=True)
        batches: List[pd.DataFrame] = [baseline]

        for batch_index in range(1, n):
            rng = self._batch_rng(batch_index)
            random_state = int(rng.integers(0, 2_147_483_647))

            batch = baseline.sample(
                n=self.rows_per_batch,
                replace=True,
                random_state=random_state,
            ).reset_index(drop=True)

            batch["transaction_amount"] = (
                batch["transaction_amount"]
                + rng.normal(loc=0.0, scale=0.75, size=self.rows_per_batch)
            )
            batch["user_age"] = (
                batch["user_age"]
                + rng.normal(loc=0.0, scale=0.30, size=self.rows_per_batch)
            )
            batch["session_duration"] = (
                batch["session_duration"]
                + rng.normal(loc=0.0, scale=0.20, size=self.rows_per_batch)
            )

            batches.append(batch)

        return batches

    def generate_corrupted_sequence(self, n: int, corrupt_at: int) -> List[pd.DataFrame]:
        """Return `n` batches, injecting corruption starting at `corrupt_at`."""
        if n <= 0:
            raise ValueError("n must be greater than 0")
        if corrupt_at < 0 or corrupt_at >= n:
            raise ValueError("corrupt_at must be in [0, n-1]")

        batches = self.generate_healthy_batches(n)
        injector = CorruptionInjector(seed=self.seed + 100_000)

        for batch_index in range(corrupt_at, n):
            batches[batch_index] = injector.inject_all(batches[batch_index])

        return batches
