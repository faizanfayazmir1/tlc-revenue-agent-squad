"""
Durable SQLite state store (`DurableStateStore`).
Guarantees that if the orchestrator process is killed mid-run (e.g., SIGKILL or
crash after profiling or after Builder Attempt 1), resuming the run reloads
committed stage checkpoints from `state/squad_state.db` without redoing finished work.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
from typing import Any, Dict, Iterator, List, Optional

from squad.models import RunStatus, TraceEvent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DurableStateStore:
    """
    SQLite-backed durable checkpoint and audit trace store.
    Transparently supports both native POSIX filesystems and virtualized 9p/NFS
    mounts by staging SQLite page locks in POSIX temp storage and atomically
    flushing the binary SQLite database to `self.db_path` (`state/squad_state.db`)
    after every transaction.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        path_hash = hashlib.sha256(str(self.db_path).encode("utf-8")).hexdigest()[:12]
        self._work_db_path = Path(tempfile.gettempdir()) / f"spyne_squad_state_{path_hash}.db"
        self._init_db()

    @contextmanager
    def _connect(self, write: bool = False) -> Iterator[sqlite3.Connection]:
        if self.db_path.exists() and self.db_path.stat().st_size > 0:
            shutil.copyfile(self.db_path, self._work_db_path)
        elif self._work_db_path.exists() and not self.db_path.exists():
            self._work_db_path.unlink(missing_ok=True)

        conn = sqlite3.connect(str(self._work_db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            if write:
                conn.commit()
        finally:
            conn.close()
            if write and self._work_db_path.exists():
                self.db_path.write_bytes(self._work_db_path.read_bytes())

    def _init_db(self) -> None:
        with self._connect(write=True) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    month TEXT NOT NULL,
                    parquet_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    steps_used INTEGER NOT NULL DEFAULT 0,
                    retries_used INTEGER NOT NULL DEFAULT 0,
                    cost_usd REAL NOT NULL DEFAULT 0.0,
                    resumed_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS stage_checkpoints (
                    run_id TEXT NOT NULL,
                    stage_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, stage_key)
                );

                CREATE TABLE IF NOT EXISTS trace_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    seq_no INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    action TEXT NOT NULL,
                    tool_used TEXT,
                    status TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );
                """
            )

    def init_or_load_run(
        self,
        run_id: str,
        month: str,
        parquet_path: str,
        reset: bool = False,
    ) -> Dict[str, Any]:
        with self._connect(write=True) as conn:
            if reset:
                conn.execute("DELETE FROM trace_events WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM stage_checkpoints WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))

            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            now = utc_now_iso()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO runs (
                        run_id, month, parquet_path, status, steps_used,
                        retries_used, cost_usd, resumed_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 0, 0, 0.0, 0, ?, ?)
                    """,
                    (run_id, month, parquet_path, RunStatus.IN_PROGRESS.value, now, now),
                )
                return {
                    "run_id": run_id,
                    "month": month,
                    "parquet_path": parquet_path,
                    "status": RunStatus.IN_PROGRESS.value,
                    "steps_used": 0,
                    "retries_used": 0,
                    "cost_usd": 0.0,
                    "resumed": False,
                }
            else:
                conn.execute(
                    "UPDATE runs SET resumed_count = resumed_count + 1, updated_at = ? WHERE run_id = ?",
                    (now, run_id),
                )
                return {
                    "run_id": row["run_id"],
                    "month": row["month"],
                    "parquet_path": row["parquet_path"],
                    "status": row["status"],
                    "steps_used": row["steps_used"],
                    "retries_used": row["retries_used"],
                    "cost_usd": row["cost_usd"],
                    "resumed": True,
                }

    def get_checkpoint(self, run_id: str, stage_key: str) -> Optional[Dict[str, Any]]:
        with self._connect(write=False) as conn:
            row = conn.execute(
                "SELECT payload_json FROM stage_checkpoints WHERE run_id = ? AND stage_key = ?",
                (run_id, stage_key),
            ).fetchone()
            if row is None:
                return None
            return json.loads(row["payload_json"])

    def save_checkpoint(
        self,
        run_id: str,
        stage_key: str,
        payload: Dict[str, Any],
        steps_delta: int = 1,
        retries_delta: int = 0,
        cost_delta_usd: float = 0.0,
    ) -> None:
        now = utc_now_iso()
        payload_str = json.dumps(payload, default=str)
        with self._connect(write=True) as conn:
            conn.execute(
                """
                INSERT INTO stage_checkpoints (run_id, stage_key, payload_json, completed_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id, stage_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    completed_at = excluded.completed_at
                """,
                (run_id, stage_key, payload_str, now),
            )
            conn.execute(
                """
                UPDATE runs
                SET steps_used = steps_used + ?,
                    retries_used = retries_used + ?,
                    cost_usd = ROUND(cost_usd + ?, 6),
                    updated_at = ?
                WHERE run_id = ?
                """,
                (steps_delta, retries_delta, cost_delta_usd, now, run_id),
            )

    def update_run_status(self, run_id: str, status: RunStatus) -> None:
        now = utc_now_iso()
        with self._connect(write=True) as conn:
            conn.execute(
                "UPDATE runs SET status = ?, updated_at = ? WHERE run_id = ?",
                (status.value, now, run_id),
            )

    def get_run_counters(self, run_id: str) -> Dict[str, Any]:
        with self._connect(write=False) as conn:
            row = conn.execute(
                "SELECT steps_used, retries_used, cost_usd, status FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return {"steps_used": 0, "retries_used": 0, "cost_usd": 0.0, "status": "UNKNOWN"}
            return dict(row)

    def append_trace_event(
        self,
        run_id: str,
        agent: str,
        stage: str,
        action: str,
        status: str,
        summary: str,
        tool_used: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> TraceEvent:
        now = utc_now_iso()
        details_dict = details or {}
        with self._connect(write=True) as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq_no), 0) + 1 AS next_seq FROM trace_events WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            seq_no = int(row["next_seq"])
            conn.execute(
                """
                INSERT INTO trace_events (
                    run_id, seq_no, timestamp, agent, stage, action,
                    tool_used, status, summary, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    seq_no,
                    now,
                    agent,
                    stage,
                    action,
                    tool_used,
                    status,
                    summary,
                    json.dumps(details_dict, default=str),
                ),
            )
        return TraceEvent(
            seq_no=seq_no,
            timestamp=now,
            agent=agent,
            stage=stage,
            action=action,
            tool_used=tool_used,
            status=status,
            summary=summary,
            details=details_dict,
        )

    def list_trace_events(self, run_id: str) -> List[TraceEvent]:
        with self._connect(write=False) as conn:
            rows = conn.execute(
                "SELECT * FROM trace_events WHERE run_id = ? ORDER BY seq_no ASC",
                (run_id,),
            ).fetchall()
            return [
                TraceEvent(
                    seq_no=r["seq_no"],
                    timestamp=r["timestamp"],
                    agent=r["agent"],
                    stage=r["stage"],
                    action=r["action"],
                    tool_used=r["tool_used"],
                    status=r["status"],
                    summary=r["summary"],
                    details=json.loads(r["details_json"]),
                )
                for r in rows
            ]
