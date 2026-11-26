"""演练控制库 drill.db：保存演练时钟、班次、调度尝试与事件。

注意：真正的 Job 真相在独立的 cluster.db；本库的 cluster_job_id 只是
“调度端收到回执/事后查回后”建立的关联，可能暂时为空（回执丢失）。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import ANCHOR_TS, SCHEMA_VERSION, SEED_REPOS

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repositories (
    id        TEXT PRIMARY KEY,
    name      TEXT NOT NULL,
    seeded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS shifts (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id                TEXT NOT NULL REFERENCES repositories(id),
    seq                    INTEGER NOT NULL,
    due_ts                 INTEGER NOT NULL,
    deadline_ts            INTEGER NOT NULL,
    idem_key               TEXT NOT NULL UNIQUE,
    status                 TEXT NOT NULL DEFAULT 'WAITING'
                           CHECK (status IN ('WAITING','CREATED','SKIPPED')),
    skip_reason            TEXT,
    cluster_job_id         TEXT,
    job_source             TEXT CHECK (job_source IN
                           ('RECEIPT','IDEMPOTENT_RETRY','LOOKUP') OR job_source IS NULL),
    pending_lost_receipt   INTEGER NOT NULL DEFAULT 0,
    recovered_after_skip   INTEGER NOT NULL DEFAULT 0,
    blocked_by_job_id      TEXT,
    note                   TEXT,
    created_real_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_real_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (repo_id, due_ts)
);
CREATE INDEX IF NOT EXISTS ix_shifts_due ON shifts(due_ts);
CREATE INDEX IF NOT EXISTS ix_shifts_status ON shifts(status);
CREATE INDEX IF NOT EXISTS ix_shifts_repo_due ON shifts(repo_id, due_ts);

CREATE TABLE IF NOT EXISTS attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shift_id        INTEGER REFERENCES shifts(id),
    idem_key        TEXT NOT NULL,
    sim_ts          INTEGER NOT NULL,
    outcome         TEXT NOT NULL,   -- RECEIVED / RECEIPT_LOST / OCCUPIED / SKIPPED / RECOVERED
    cluster_job_id  TEXT,
    detail          TEXT,
    real_at         TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_attempts_shift ON attempts(shift_id);

CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sim_ts   INTEGER NOT NULL,
    level    TEXT NOT NULL DEFAULT 'info',  -- info / success / warn / danger
    repo_id  TEXT,
    message  TEXT NOT NULL,
    real_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=20.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=20000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_schema(self) -> None:
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('sim_now_ts', ?)",
                (str(ANCHOR_TS),),
            )
            for repo_id, name in SEED_REPOS:
                conn.execute(
                    "INSERT OR IGNORE INTO repositories(id, name) VALUES(?, ?)",
                    (repo_id, name),
                )
        finally:
            conn.close()

    # ---- meta ---------------------------------------------------------------
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
            return row["value"] if row else default
        finally:
            conn.close()

    def get_sim_now(self) -> int:
        return int(self.get_meta("sim_now_ts", str(ANCHOR_TS)))  # type: ignore[arg-type]

    # ---- repos --------------------------------------------------------------
    def list_repos(self) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            return list(
                conn.execute("SELECT * FROM repositories ORDER BY id").fetchall()
            )
        finally:
            conn.close()

    # ---- events -------------------------------------------------------------
    def add_event(
        self,
        conn: sqlite3.Connection,
        *,
        sim_ts: int,
        message: str,
        level: str = "info",
        repo_id: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO events(sim_ts, level, repo_id, message) VALUES(?,?,?,?)",
            (int(sim_ts), level, repo_id, message),
        )

    def list_events(self, limit: int = 40) -> list[dict]:
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (int(limit),)
            ).fetchall()
            return [{k: r[k] for k in r.keys()} for r in rows]
        finally:
            conn.close()
