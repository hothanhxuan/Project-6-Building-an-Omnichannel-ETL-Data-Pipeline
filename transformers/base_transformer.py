"""
Base Transformer - Provides data quality checks and common transformation utilities.
All transformers inherit from this class.
"""

import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
from utils.logger import setup_logger


class BaseTransformer:
    """Base class for all data transformers with built-in data quality checks."""

    def __init__(self):
        """Initialize base transformer with logger."""
        self.logger = setup_logger(self.__class__.__name__)
        self.quality_report = {}

    # ──────────────────────────────────────────────
    # Data Quality Checks
    # ──────────────────────────────────────────────

    def check_nulls(self, df: pd.DataFrame, columns: list, table_name: str = "") -> pd.DataFrame:
        """
        Check for null values in critical columns and log warnings.

        Args:
            df: DataFrame to check.
            columns: List of column names to check for nulls.
            table_name: Name of the table for logging.

        Returns:
            Original DataFrame (nulls are logged, not removed).
        """
        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    pct = (null_count / len(df)) * 100
                    self.logger.warning(
                        f"[{table_name}] Column '{col}' has {null_count} null values ({pct:.2f}%)"
                    )
                    self.quality_report[f"{table_name}.{col}.nulls"] = null_count
        return df

    def check_duplicates(self, df: pd.DataFrame, key_columns: list, table_name: str = "") -> pd.DataFrame:
        """
        Check for and remove duplicate records based on key columns.

        Args:
            df: DataFrame to check.
            key_columns: Columns that define uniqueness.
            table_name: Name of the table for logging.

        Returns:
            DataFrame with duplicates removed.
        """
        existing_cols = [c for c in key_columns if c in df.columns]
        if not existing_cols:
            return df

        dup_count = df.duplicated(subset=existing_cols, keep="first").sum()
        if dup_count > 0:
            self.logger.warning(
                f"[{table_name}] Found {dup_count} duplicate records on {existing_cols}. Removing..."
            )
            df = df.drop_duplicates(subset=existing_cols, keep="first").reset_index(drop=True)
            self.quality_report[f"{table_name}.duplicates"] = dup_count
        else:
            self.logger.info(f"[{table_name}] No duplicates found on {existing_cols}")
        return df

    def validate_date_range(self, df: pd.DataFrame, date_column: str,
                            min_date: str = "2020-01-01", max_date: str = None,
                            table_name: str = "") -> pd.DataFrame:
        """
        Validate that dates fall within a reasonable range.

        Args:
            df: DataFrame to check.
            date_column: Name of the date column.
            min_date: Minimum acceptable date string.
            max_date: Maximum acceptable date (defaults to now + 1 day).
            table_name: Name of the table for logging.

        Returns:
            Original DataFrame (invalid dates are logged as warnings).
        """
        if date_column not in df.columns:
            return df

        if max_date is None:
            max_date = (datetime.now() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        dates = pd.to_datetime(df[date_column], errors="coerce")
        min_dt = pd.Timestamp(min_date)
        max_dt = pd.Timestamp(max_date)

        out_of_range = ((dates < min_dt) | (dates > max_dt)).sum()
        future_dates = (dates > pd.Timestamp.now()).sum()

        if out_of_range > 0:
            self.logger.warning(
                f"[{table_name}] Column '{date_column}': {out_of_range} dates outside "
                f"range [{min_date}, {max_date}]"
            )
        if future_dates > 0:
            self.logger.warning(
                f"[{table_name}] Column '{date_column}': {future_dates} future dates detected"
            )

        actual_min = dates.min()
        actual_max = dates.max()
        self.logger.info(
            f"[{table_name}] Date range for '{date_column}': {actual_min} to {actual_max}"
        )
        return df

    def validate_amounts(self, df: pd.DataFrame, amount_columns: list,
                         allow_negative: bool = False, table_name: str = "") -> pd.DataFrame:
        """
        Validate amount columns for negative values and outliers.

        Args:
            df: DataFrame to check.
            amount_columns: List of numeric columns to validate.
            allow_negative: Whether negative values are acceptable.
            table_name: Name of the table for logging.

        Returns:
            Original DataFrame (issues are logged as warnings).
        """
        for col in amount_columns:
            if col not in df.columns:
                continue

            numeric_col = pd.to_numeric(df[col], errors="coerce")

            if not allow_negative:
                neg_count = (numeric_col < 0).sum()
                if neg_count > 0:
                    self.logger.warning(
                        f"[{table_name}] Column '{col}': {neg_count} negative values found"
                    )

            # Statistical summary
            mean_val = numeric_col.mean()
            median_val = numeric_col.median()
            std_val = numeric_col.std()
            self.logger.info(
                f"[{table_name}] '{col}' stats: mean={mean_val:,.0f}, "
                f"median={median_val:,.0f}, std={std_val:,.0f}"
            )

            # Check for outliers (> 3 standard deviations)
            if std_val > 0:
                outliers = ((numeric_col - mean_val).abs() > 3 * std_val).sum()
                if outliers > 0:
                    self.logger.warning(
                        f"[{table_name}] Column '{col}': {outliers} potential outliers (>3σ)"
                    )
        return df

    # ──────────────────────────────────────────────
    # Transformation Utilities
    # ──────────────────────────────────────────────

    @staticmethod
    def generate_surrogate_key(*args) -> str:
        """
        Generate a deterministic surrogate key from input values using MD5 hash.

        Args:
            *args: Values to hash together.

        Returns:
            8-character hexadecimal string key.
        """
        key_string = "|".join(str(a) for a in args)
        return hashlib.md5(key_string.encode()).hexdigest()[:16]

    @staticmethod
    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names: lowercase, replace spaces with underscores.

        Args:
            df: DataFrame to standardize.

        Returns:
            DataFrame with standardized column names.
        """
        df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]
        return df

    @staticmethod
    def parse_datetime(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Parse datetime columns to pandas Timestamp.

        Args:
            df: DataFrame to process.
            columns: List of column names to parse as datetime.

        Returns:
            DataFrame with parsed datetime columns.
        """
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                # Convert to timezone-naive for BigQuery compatibility
                df[col] = df[col].dt.tz_localize(None)
        return df

    @staticmethod
    def create_date_key(df: pd.DataFrame, date_column: str, key_column: str) -> pd.DataFrame:
        """
        Create an integer date key (YYYYMMDD) from a datetime column.

        Args:
            df: DataFrame to process.
            date_column: Source datetime column name.
            key_column: Target date key column name.

        Returns:
            DataFrame with the new date key column.
        """
        if date_column in df.columns:
            dates = pd.to_datetime(df[date_column], errors="coerce")
            df[key_column] = dates.dt.strftime("%Y%m%d").fillna("0").astype(int)
        return df

    def get_quality_report(self) -> dict:
        """Return the accumulated data quality report."""
        return self.quality_report
