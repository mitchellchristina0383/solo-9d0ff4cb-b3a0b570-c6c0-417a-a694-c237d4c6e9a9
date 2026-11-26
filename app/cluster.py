"""模拟备份集群。

职责边界：
- 这是“集群侧”的独立 SQLite 库（cluster.db），与演练控制库 drill.db 分开保存；
- Job 一旦在此提交，就算调度端把“回执”丢了，Job 也真实存在，可按幂等键查回；
- 两条数据库级硬约束：
  1. jobs.idempotency_key UNIQUE —— 同一班重试/并发不会建出两个 Job；
  2. 部分唯一索引 (repo_id) WHERE status='RUNNING' —— 同一备份库同时至多一个运行 Job。

所有写事务都用 BEGIN IMMEDIATE 串行化“检查占用 -> 插入”，
并发调度最终以“实际创建那一刻”的事务检查与约束为准。
"""
from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id           TEXT PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    repo_id          TEXT NOT NULL,
    shift_due_ts     INTEGER NOT NULL,
    kind             TEXT NOT NULL DEFAULT 'BACKUP_VERIFY',
    status           TEXT NOT NULL DEFAULT 'RUNNING'
                     CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    created_ts       INTEGER NOT NULL,          -- 实际创建时的演练模拟时间
    ended_ts         INTEGER,
    end_result       TEXT,
    creator          TEXT NOT NULL DEFAULT 'scheduler',
    payload          TEXT
);

-- 同一备份库不能同时有两个 RUNNING 的 Job（集群侧最终裁决）
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_one_running_per_repo
    ON jobs(repo_id) WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS ix_jobs_repo_status ON jobs(repo_id, status);
CREATE INDEX IF NOT EXISTS ix_jobs_idem ON jobs(idempotency_key);

-- 集群模拟器自身的开关：一次性“创建成功但回执丢失”故障也原子保存在集群库，
-- 保证多 worker / 重启场景下故障恰好生效一次。
CREATE TABLE IF NOT EXISTS cluster_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class RepoOccupied(Exception):
    """实际创建时发现该备份库已有 RUNNING Job。"""

    def __init__(self, repo_id: str, running_job_id: str):
        super().__init__(f"repo {repo_id} occupied by {running_job_id}")
        self.repo_id = repo_id
        self.running_job_id = running_job_id


class ReceiptLost(Exception):
    """Job 已在集群创建并提交，但返回调度端的回执在“网络”中丢失。

    刻意不携带 job_id：调度端在丢回执时不应知道集群 Job ID，
    事后只能凭 idempotency_key 向集群查回。
    """

    def __init__(self, idempotency_key: str):
        super().__init__(f"receipt lost for idem_key={idempotency_key}")
        self.idempotency_key = idempotency_key


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


class ClusterDB:
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
        finally:
            conn.close()

    # ---- 故障注入 -----------------------------------------------------------
    def fault_key(self, repo_id: str) -> str:
        return f"lose_next_receipt:{repo_id}"

    def arm_lost_receipt(self, repo_id: str, armed: bool = True) -> None:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if armed:
                conn.execute(
                    "INSERT INTO cluster_meta(key, value) VALUES(?, '1') "
                    "ON CONFLICT(key) DO UPDATE SET value='1'",
                    (self.fault_key(repo_id),),
                )
            else:
                conn.execute(
                    "DELETE FROM cluster_meta WHERE key=?",
                    (self.fault_key(repo_id),),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def is_lost_receipt_armed(self, repo_id: str) -> bool:
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM cluster_meta WHERE key=?",
                (self.fault_key(repo_id),),
            ).fetchone()
            return row is not None
        except sqlite3.OperationalError:
            # 表尚未初始化时
            return False
        finally:
            conn.close()

    # ---- Job 操作 -----------------------------------------------------------
    def create_job(
        self,
        *,
        idempotency_key: str,
        repo_id: str,
        shift_due_ts: int,
        sim_now_ts: int,
    ) -> dict[str, Any]:
        """在集群中创建 Job。

        返回 Job dict（额外带 replayed 字段表示是幂等重放）。
        可能抛出 RepoOccupied / ReceiptLost。
        """
        conn = self.connect()
        lost = False
        try:
            # 实际创建时的时间与占用，都在这个立即写事务里裁决
            conn.execute("BEGIN IMMEDIATE")

            existing = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                job = _row_to_dict(existing)
                assert job is not None
                job["replayed"] = True
                return job

            running = conn.execute(
                "SELECT job_id FROM jobs WHERE repo_id=? AND status='RUNNING' LIMIT 1",
                (repo_id,),
            ).fetchone()
            if running is not None:
                conn.execute("ROLLBACK")
                raise RepoOccupied(repo_id, running["job_id"])

            job_id = f"job-{uuid.uuid4().hex[:12]}"
            try:
                conn.execute(
                    """
                    INSERT INTO jobs
                        (job_id, idempotency_key, repo_id, shift_due_ts,
                         status, created_ts, creator, payload)
                    VALUES (?, ?, ?, ?, 'RUNNING', ?, 'scheduler', ?)
                    """,
                    (
                        job_id,
                        idempotency_key,
                        repo_id,
                        int(shift_due_ts),
                        int(sim_now_ts),
                        f"backup-verification shift due {shift_due_ts}",
                    ),
                )
            except sqlite3.IntegrityError:
                # 并发兜底：唯一键冲突可能是同幂等键，也可能撞部分唯一索引
                conn.execute("ROLLBACK")
                dup = conn.execute(
                    "SELECT * FROM jobs WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
                if dup is not None:
                    job = _row_to_dict(dup)
                    assert job is not None
                    job["replayed"] = True
                    return job
                run = conn.execute(
                    "SELECT job_id FROM jobs WHERE repo_id=? AND status='RUNNING' LIMIT 1",
                    (repo_id,),
                ).fetchone()
                raise RepoOccupied(repo_id, run["job_id"] if run else "unknown")

            # 与插入同一事务消费一次性故障开关：恰好丢一次回执
            cur = conn.execute(
                "DELETE FROM cluster_meta WHERE key=?",
                (self.fault_key(repo_id),),
            )
            if cur.rowcount and cur.rowcount > 0:
                lost = True

            conn.execute("COMMIT")
        except RepoOccupied:
            raise
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

        if lost:
            # 集群侧 Job 已提交，仅回执丢失；异常刻意不带 job_id
            raise ReceiptLost(idempotency_key)

        job = self.get_job_by_idem(idempotency_key)
        assert job is not None
        job["replayed"] = False
        return job

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        conn = self.connect()
        try:
            return _row_to_dict(
                conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            )
        finally:
            conn.close()

    def get_job_by_idem(self, idempotency_key: str) -> dict[str, Any] | None:
        conn = self.connect()
        try:
            return _row_to_dict(
                conn.execute(
                    "SELECT * FROM jobs WHERE idempotency_key=?",
                    (idempotency_key,),
                ).fetchone()
            )
        finally:
            conn.close()

    def find_running(self, repo_id: str) -> dict[str, Any] | None:
        conn = self.connect()
        try:
            return _row_to_dict(
                conn.execute(
                    "SELECT * FROM jobs WHERE repo_id=? AND status='RUNNING' LIMIT 1",
                    (repo_id,),
                ).fetchone()
            )
        finally:
            conn.close()

    def list_jobs(self, limit: int = 500) -> list[dict[str, Any]]:
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY created_ts DESC, rowid DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
            return [_row_to_dict(r) for r in rows]  # type: ignore[misc]
        finally:
            conn.close()

    def finish_job(
        self, job_id: str, sim_now_ts: int, result: str = "SUCCEEDED"
    ) -> dict[str, Any] | None:
        assert result in ("SUCCEEDED", "FAILED")
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return None
            if row["status"] != "RUNNING":
                conn.execute("COMMIT")
                return _row_to_dict(row)
            conn.execute(
                "UPDATE jobs SET status=?, ended_ts=?, end_result=? WHERE job_id=?",
                (result, int(sim_now_ts), result, job_id),
            )
            conn.execute("COMMIT")
            return self.get_job(job_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def count_jobs(self) -> int:
        conn = self.connect()
        try:
            return int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
        finally:
            conn.close()
