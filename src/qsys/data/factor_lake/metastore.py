from __future__ import annotations

import sqlite3
from pathlib import Path


class FactorLakeMetastore:
    """SQLite metastore for raw factor lake inventory and ingest logs."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_tables(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                create table if not exists sync_meta(
                    run_id text,
                    dataset text,
                    started_at text,
                    ended_at text,
                    status text,
                    primary key(run_id, dataset)
                )
                """
            )
            conn.execute(
                """
                create table if not exists raw_dataset_inventory(
                    dataset text,
                    source_family text,
                    api_name text,
                    partition_json text,
                    data_path text,
                    metadata_path text,
                    row_count integer,
                    col_count integer,
                    primary key(dataset, api_name, partition_json)
                )
                """
            )
            conn.execute(
                """
                create table if not exists ingest_run_log(
                    run_id text,
                    dataset text,
                    api_name text,
                    partition_json text,
                    status text,
                    error_message text,
                    elapsed_seconds real,
                    created_at text
                )
                """
            )

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._connect() as conn:
            conn.execute(sql, params)
