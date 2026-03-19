"""Synthetic corruption utilities used by end-to-end benchmark tests."""

from typing import Optional

import numpy as np
import pandas as pd


class CorruptionInjector:
    """Inject realistic corruption patterns into DataFrame copies."""

    def __init__(self, seed: int = 2026) -> None:
        self._rng = np.random.default_rng(seed)

    def inject_null_explosion(self, df: pd.DataFrame, column: str, rate: float) -> pd.DataFrame:
        """Return a copy of `df` with `rate` share of `column` set to None."""
        if column not in df.columns:
            raise KeyError("Column '{0}' does not exist.".format(column))
        if rate < 0.0 or rate > 1.0:
            raise ValueError("rate must be between 0.0 and 1.0")

        corrupted = df.copy(deep=True)
        if len(corrupted) == 0 or rate == 0.0:
            return corrupted

        count = int(round(len(corrupted) * rate))
        if count <= 0:
            return corrupted

        indices = self._rng.choice(corrupted.index.to_numpy(), size=count, replace=False)
        corrupted.loc[indices, column] = None
        return corrupted

    def inject_distribution_shift(
        self,
        df: pd.DataFrame,
        column: str,
        shift_mean: float,
        shift_std: float,
    ) -> pd.DataFrame:
        """Return a copy with `column` replaced by a shifted normal distribution."""
        if column not in df.columns:
            raise KeyError("Column '{0}' does not exist.".format(column))
        if shift_std <= 0:
            raise ValueError("shift_std must be > 0")

        corrupted = df.copy(deep=True)
        corrupted[column] = self._rng.normal(
            loc=float(shift_mean),
            scale=float(shift_std),
            size=len(corrupted),
        )
        return corrupted

    def inject_schema_drift(
        self,
        df: pd.DataFrame,
        action: str,
        column_name: str,
        new_dtype: str,
    ) -> pd.DataFrame:
        """Return a copy with schema drift: add, remove, or type-change a column."""
        action_normalized = str(action).strip().lower()
        dtype_normalized = str(new_dtype).strip().lower()
        corrupted = df.copy(deep=True)

        if action_normalized == "add":
            if column_name in corrupted.columns:
                raise ValueError("Column '{0}' already exists for add action.".format(column_name))

            if dtype_normalized in ("float", "float64"):
                values = self._rng.normal(loc=0.0, scale=1.0, size=len(corrupted))
            elif dtype_normalized in ("int", "int64"):
                values = self._rng.integers(0, 1000, size=len(corrupted))
            elif dtype_normalized in ("bool", "boolean"):
                values = self._rng.choice([True, False], size=len(corrupted), p=[0.5, 0.5])
            else:
                values = np.array(
                    ["{0}_{1}".format(column_name, i % 5) for i in range(len(corrupted))],
                    dtype=object,
                )

            corrupted[column_name] = values
            return corrupted

        if action_normalized == "remove":
            if column_name not in corrupted.columns:
                raise KeyError("Column '{0}' does not exist for remove action.".format(column_name))
            return corrupted.drop(columns=[column_name])

        if action_normalized == "change":
            if column_name not in corrupted.columns:
                raise KeyError("Column '{0}' does not exist for change action.".format(column_name))

            if dtype_normalized in ("str", "string", "object"):
                corrupted[column_name] = corrupted[column_name].astype(str)
            elif dtype_normalized in ("int", "int64"):
                corrupted[column_name] = corrupted[column_name].fillna(0).astype(int)
            elif dtype_normalized in ("float", "float64"):
                corrupted[column_name] = corrupted[column_name].astype(float)
            elif dtype_normalized in ("bool", "boolean"):
                corrupted[column_name] = corrupted[column_name].astype(bool)
            else:
                corrupted[column_name] = corrupted[column_name].astype(new_dtype)

            return corrupted

        raise ValueError("Unsupported schema drift action '{0}'.".format(action))

    def inject_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a copy with null, distribution, and schema corruption combined."""
        corrupted = df.copy(deep=True)

        numeric_columns = corrupted.select_dtypes(include=[np.number]).columns.tolist()

        null_column: Optional[str] = None
        if "transaction_amount" in corrupted.columns:
            null_column = "transaction_amount"
        elif numeric_columns:
            null_column = numeric_columns[0]

        if null_column is not None:
            corrupted = self.inject_null_explosion(corrupted, column=null_column, rate=0.40)

        shift_column: Optional[str] = None
        if "user_age" in corrupted.columns:
            shift_column = "user_age"
        elif len(numeric_columns) > 1:
            shift_column = numeric_columns[1]
        elif numeric_columns:
            shift_column = numeric_columns[0]

        if shift_column is not None:
            corrupted = self.inject_distribution_shift(
                corrupted,
                column=shift_column,
                shift_mean=65.0,
                shift_std=8.0,
            )

        schema_column = "schema_new_col"
        if schema_column in corrupted.columns:
            schema_column = "schema_new_col_{0}".format(self._rng.integers(1, 1000))

        corrupted = self.inject_schema_drift(
            corrupted,
            action="add",
            column_name=schema_column,
            new_dtype="int",
        )

        return corrupted
