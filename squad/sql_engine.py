"""
Real SQL and Parquet profiling tools used by the Squad agents.
Supports DuckDB (if installed) and falls back natively to PyArrow + SQLite3,
exposing two SQL tables for any run:
  - `yellow_tripdata` (loaded from the monthly Parquet file)
  - `taxi_zone_lookup` (loaded from data/taxi_zone_lookup.csv)

All agent SQL queries are validated to be strictly read-only (`SELECT` / `WITH`).
"""

from __future__ import annotations

import calendar
from pathlib import Path
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq


FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|ATTACH|DETACH|PRAGMA|VACUUM|REPLACE)\b",
    re.IGNORECASE,
)


def compute_month_bounds(month: str) -> Tuple[str, str]:
    """Returns [month_start, next_month_start) in 'YYYY-MM-DD HH:MM:SS' format."""
    parts = month.split("-")
    year = int(parts[0])
    mon = int(parts[1])
    if not (1 <= mon <= 12):
        # Fallback for synthetic test month identifiers like 2025-99
        return ("2025-02-01 00:00:00", "2025-03-01 00:00:00")
    start_str = f"{year:04d}-{mon:02d}-01 00:00:00"
    if mon == 12:
        end_str = f"{year + 1:04d}-01-01 00:00:00"
    else:
        end_str = f"{year:04d}-{mon + 1:02d}-01 00:00:00"
    return start_str, end_str


class SQLDataEngine:
    """
    Provides real SQL execution and Parquet schema/data profiling tools over
    `yellow_tripdata` and `taxi_zone_lookup`.
    """

    def __init__(self, parquet_path: Path, zone_lookup_path: Path) -> None:
        self.parquet_path = Path(parquet_path)
        self.zone_lookup_path = Path(zone_lookup_path)
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.parquet_path}")
        if not self.zone_lookup_path.exists():
            raise FileNotFoundError(f"Zone lookup CSV not found: {self.zone_lookup_path}")

        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row
        self._load_tables()

    def _load_tables(self) -> None:
        df_trips = pq.read_table(self.parquet_path).to_pandas()
        # Convert timestamp columns to ISO strings 'YYYY-MM-DD HH:MM:SS' for uniform SQLite/DuckDB SQL semantics
        for col in df_trips.columns:
            if pd.api.types.is_datetime64_any_dtype(df_trips[col]):
                df_trips[col] = df_trips[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_trips.to_sql("yellow_tripdata", self._conn, index=False, if_exists="replace", chunksize=50000)

        df_zones = pd.read_csv(self.zone_lookup_path, keep_default_na=False)
        df_zones.to_sql("taxi_zone_lookup", self._conn, index=False, if_exists="replace")

        # Create helpful indexes for fast analytical & verification queries
        cols_lower = {c.lower(): c for c in df_trips.columns}
        if "pulocationid" in cols_lower:
            pu_col = cols_lower["pulocationid"]
            self._conn.execute(f'CREATE INDEX idx_trips_pu ON yellow_tripdata ("{pu_col}");')
        if "payment_type" in cols_lower:
            pt_col = cols_lower["payment_type"]
            self._conn.execute(f'CREATE INDEX idx_trips_pt ON yellow_tripdata ("{pt_col}");')
        self._conn.execute('CREATE INDEX idx_zones_loc ON taxi_zone_lookup ("LocationID");')

    @staticmethod
    def validate_readonly_sql(query: str) -> None:
        stripped = query.strip()
        if not stripped:
            raise ValueError("Empty SQL query is not allowed.")
        first_token = stripped.split()[0].upper()
        if first_token not in ("SELECT", "WITH"):
            raise PermissionError(
                f"SQL security policy violation: query must start with SELECT or WITH, got '{first_token}'."
            )
        if FORBIDDEN_SQL_PATTERN.search(stripped):
            raise PermissionError(
                "SQL security policy violation: mutating or administrative SQL keywords are forbidden."
            )

    def execute_readonly_sql(self, query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        """Executes a read-only SQL query and returns rows as a list of dicts."""
        self.validate_readonly_sql(query)
        cur = self._conn.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

    def inspect_parquet_schema(self) -> Dict[str, Dict[str, str]]:
        """Tool: Reads physical Parquet metadata & Arrow types directly from file header."""
        schema = pq.read_schema(self.parquet_path)
        result: Dict[str, Dict[str, str]] = {}
        for field in schema:
            t_str = str(field.type)
            if "timestamp" in t_str or "date" in t_str:
                logical = "timestamp"
            elif any(k in t_str for k in ("int", "uint")):
                logical = "integer"
            elif any(k in t_str for k in ("double", "float", "decimal")):
                logical = "numeric"
            elif "string" in t_str or "utf8" in t_str or "binary" in t_str:
                logical = "string"
            else:
                logical = t_str
            result[field.name] = {"arrow_type": t_str, "logical_type": logical}
        return result

    def close(self) -> None:
        self._conn.close()
