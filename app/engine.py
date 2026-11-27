"""调度演练核心：班次状态机 + 时钟推进 + 集群交互 + 回执查回。

班次状态：WAITING -> CREATED / SKIPPED（库内三态，页面对应等待/已建/跳过）。
关键规则：
1. 班次每 600s 一班，补建窗口 180s；now >= due+180 即“满 3 分钟”，跳过。
2. 窗口内若同备份库集群侧已有 RUNNING Job（通常是前一班），本班保持 WAITING，
   等下一次触发；前班结束后、窗口未满时触发即可补建。
3. 每班一个幂等键；集群侧 UNIQUE + 同库 RUNNING 部分唯一索引兜底并发。
4. 所有判定在控制库 BEGIN IMMEDIATE 事务中重读模拟时钟后做出，
   实际创建时间/占用以集群创建事务为准。
5. 丢回执只影响“回执”，集群 Job 已提交；任何时候都可按幂等键查回。
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

from .cluster import ClusterDB, ReceiptLost, RepoOccupied
from .config import (
    ANCHOR_TS,
    CLUSTER_DB_PATH,
    CONTROL_DB_PATH,
    LOOKAHEAD_SEC,
    MAKEUP_WINDOW_SEC,
    SHIFT_INTERVAL_SEC,
)
from .store import Store


class NotFound(Exception):
    pass


class ValidationError(Exception):
    pass


SKIP_REASON_TEXT = {
    "WINDOW_EXPIRED": "超过 3 分钟补建窗口，且无运行中占用",
    "BUSY_TIMEOUT": "前一班 Job 一直运行，等满 3 分钟仍未结束，跳过本班",
    "LOST_RECEIPT_EXPIRED": "创建回执丢失，补跑窗口也已到期；可向集群查回原 Job",
}

JOB_SOURCE_TEXT = {
    "RECEIPT": "正常回执",
    "IDEMPOTENT_RETRY": "窗口内幂等重试返回原 Job",
    "LOOKUP": "按幂等键从集群查回",
}


class Engine:
    def __init__(
        self,
        control_path: str | Path = CONTROL_DB_PATH,
        cluster_path: str | Path = CLUSTER_DB_PATH,
    ):
        self.store = Store(control_path)
        self.cluster = ClusterDB(cluster_path)
        self.store.init_schema()
        self.cluster.init_schema()
        self._ensure_all_shifts()

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _horizon_key(repo_id: str) -> str:
        return f"cleanup_horizon:{repo_id}"

    def _cleanup_horizon(self, conn: sqlite3.Connection, repo_id: str) -> int:
        """该库的清理水位线（due_ts 上限）：<= 水位的历史班次已被清理，不再重建。

        每库只有一条 meta 记录，不逐班保留删除标记。
        """
        row = conn.execute(
            "SELECT value FROM meta WHERE key=?", (self._horizon_key(repo_id),)
        ).fetchone()
        return int(row["value"]) if row else 0

    def _ensure_shifts_locked(
        self, conn: sqlite3.Connection, now_ts: int
    ) -> int:
        """生成从锚点到 now+LOOKAHEAD 的全部班次（幂等）。返回新插入数量。

        已被清理的历史（due_ts <= 该库清理水位线）直接跳过：既不逐班重扫，
        也不会在重启/推进时钟/再次调度后把已删班次重新生成。
        """
        target = int(now_ts) + LOOKAHEAD_SEC
        k_max = max(0, (target - ANCHOR_TS) // SHIFT_INTERVAL_SEC)
        repos = [r["id"] for r in conn.execute("SELECT id FROM repositories").fetchall()]
        inserted = 0
        for repo_id in repos:
            horizon = self._cleanup_horizon(conn, repo_id)
            k_min = 0
            if horizon:
                k_min = max(0, (horizon - ANCHOR_TS) // SHIFT_INTERVAL_SEC + 1)
            for k in range(k_min, k_max + 1):
                due_ts = ANCHOR_TS + k * SHIFT_INTERVAL_SEC
                idem = f"backup-verify:{repo_id}:{due_ts}"
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO shifts
                        (repo_id, seq, due_ts, deadline_ts, idem_key, status)
                    VALUES (?, ?, ?, ?, ?, 'WAITING')
                    """,
                    (repo_id, k + 1, due_ts, due_ts + MAKEUP_WINDOW_SEC, idem),
                )
                inserted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        return inserted

    def _ensure_all_shifts(self) -> None:
        now = self.store.get_sim_now()
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_shifts_locked(conn, now)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------- clock/fault
    def advance_clock(self, seconds: int) -> dict[str, Any]:
        try:
            seconds = int(seconds)
        except (TypeError, ValueError):
            raise ValidationError("seconds 必须是整数")
        if seconds <= 0:
            raise ValidationError("只能向前推进正整数秒")
        if seconds > 24 * 3600:
            raise ValidationError("单次最多推进 86400 秒")

        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT value FROM meta WHERE key='sim_now_ts'"
            ).fetchone()
            now = int(row["value"])
            new_now = now + seconds
            conn.execute(
                "UPDATE meta SET value=? WHERE key='sim_now_ts'", (str(new_now),)
            )
            self._ensure_shifts_locked(conn, new_now)
            self.store.add_event(
                conn,
                sim_ts=new_now,
                level="info",
                message=f"值班员推进模拟时钟 +{seconds}s（{seconds // 60} 分 {seconds % 60} 秒）",
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return self.state()

    def set_fault(self, repo_id: str, armed: bool) -> dict[str, Any]:
        repos = {r["id"]: r for r in self.store.list_repos()}
        if repo_id not in repos:
            raise NotFound(f"未知备份库 {repo_id}")
        self.cluster.arm_lost_receipt(repo_id, bool(armed))

        now = self.store.get_sim_now()
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if armed:
                self.store.add_event(
                    conn,
                    sim_ts=now,
                    level="warn",
                    repo_id=repo_id,
                    message="已布置一次性故障：该库下一次 Job 在集群创建成功后丢失回执（仅一次）",
                )
            else:
                self.store.add_event(
                    conn,
                    sim_ts=now,
                    level="info",
                    repo_id=repo_id,
                    message="已取消“丢回执”故障布置",
                )
            conn.execute("COMMIT")
        finally:
            conn.close()
        return self.state()

    # ------------------------------------------------------------- scheduling
    def run_scheduling(self, repo_id: str | None = None) -> dict[str, Any]:
        """执行一轮调度。并发调用由控制库 BEGIN IMMEDIATE 串行化。"""
        summary: dict[str, Any] = {
            "created": [],
            "blocked": [],
            "skipped": [],
            "receipt_lost": [],
            "errors": [],
            "repo_id": repo_id,
        }
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")

            # 实际判定前在写事务内重读时钟（推进时钟也需要同一把写锁）
            now = int(
                conn.execute(
                    "SELECT value FROM meta WHERE key='sim_now_ts'"
                ).fetchone()["value"]
            )
            self._ensure_shifts_locked(conn, now)

            sql = (
                "SELECT * FROM shifts WHERE status='WAITING' AND due_ts <= ? "
            )
            params: list[Any] = [now]
            if repo_id:
                sql += "AND repo_id = ? "
                params.append(repo_id)
            sql += "ORDER BY repo_id ASC, due_ts ASC, id ASC"
            shifts = conn.execute(sql, params).fetchall()

            for row in shifts:
                s = {k: row[k] for k in row.keys()}
                sid = s["id"]
                rid = s["repo_id"]
                idem = s["idem_key"]

                # 1) 满 3 分钟窗口 -> 跳过（保留丢回执标记，事后仍可查回）
                if now >= s["deadline_ts"]:
                    if s["pending_lost_receipt"]:
                        reason = "LOST_RECEIPT_EXPIRED"
                    else:
                        occupied = self.cluster.find_running(rid)
                        reason = "BUSY_TIMEOUT" if occupied else "WINDOW_EXPIRED"
                    conn.execute(
                        "UPDATE shifts SET status='SKIPPED', skip_reason=?, "
                        "blocked_by_job_id=NULL, updated_real_at=datetime('now') "
                        "WHERE id=? AND status='WAITING'",
                        (reason, sid),
                    )
                    conn.execute(
                        "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, detail) "
                        "VALUES(?,?,?, 'SKIPPED', ?)",
                        (sid, idem, now, reason),
                    )
                    self.store.add_event(
                        conn,
                        sim_ts=now,
                        level="warn" if reason != "LOST_RECEIPT_EXPIRED" else "danger",
                        repo_id=rid,
                        message=(
                            f"第 {s['seq']} 班（due {s['due_ts']}）判定跳过："
                            f"{SKIP_REASON_TEXT[reason]}"
                        ),
                    )
                    summary["skipped"].append(
                        {"shift_id": sid, "repo_id": rid, "reason": reason}
                    )
                    continue

                # 2) 调度端虽未关联 Job，但集群可能已有同幂等键的 Job
                #    （丢回执后的窗口内重试走这里，保证每班至多一个 Job）
                existing = self.cluster.get_job_by_idem(idem)
                if existing is not None:
                    conn.execute(
                        "UPDATE shifts SET status='CREATED', cluster_job_id=?, "
                        "job_source='IDEMPOTENT_RETRY', pending_lost_receipt=0, "
                        "blocked_by_job_id=NULL, skip_reason=NULL, "
                        "updated_real_at=datetime('now') WHERE id=?",
                        (existing["job_id"], sid),
                    )
                    conn.execute(
                        "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, "
                        "cluster_job_id, detail) VALUES(?,?,?, 'RECEIVED', ?, ?)",
                        (sid, idem, now, existing["job_id"], "idempotent replay"),
                    )
                    self.store.add_event(
                        conn,
                        sim_ts=now,
                        level="success",
                        repo_id=rid,
                        message=(
                            f"第 {s['seq']} 班幂等重试命中集群原 Job "
                            f"{existing['job_id']}，完成关联，未重复创建"
                        ),
                    )
                    summary["created"].append(
                        {
                            "shift_id": sid,
                            "repo_id": rid,
                            "job_id": existing["job_id"],
                            "source": "IDEMPOTENT_RETRY",
                        }
                    )
                    continue

                # 3) 同库前班仍在运行 -> 本班等待（不建 Job）
                occupied = self.cluster.find_running(rid)
                if occupied is not None:
                    if s["blocked_by_job_id"] != occupied["job_id"]:
                        conn.execute(
                            "UPDATE shifts SET blocked_by_job_id=?, "
                            "updated_real_at=datetime('now') WHERE id=?",
                            (occupied["job_id"], sid),
                        )
                        self.store.add_event(
                            conn,
                            sim_ts=now,
                            level="info",
                            repo_id=rid,
                            message=(
                                f"第 {s['seq']} 班等待：同库已有运行中 Job "
                                f"{occupied['job_id']}（通常为前一班），"
                                f"将在其结束后、3 分钟窗口内补建"
                            ),
                        )
                    summary["blocked"].append(
                        {
                            "shift_id": sid,
                            "repo_id": rid,
                            "blocked_by_job_id": occupied["job_id"],
                        }
                    )
                    continue

                # 4) 窗口有效且表面空闲 -> 真正在集群事务里创建。
                #    时间以本事务读到的 now 为准，占用以集群插入事务为准。
                try:
                    job = self.cluster.create_job(
                        idempotency_key=idem,
                        repo_id=rid,
                        shift_due_ts=s["due_ts"],
                        sim_now_ts=now,
                    )
                except ReceiptLost as exc:
                    conn.execute(
                        "UPDATE shifts SET pending_lost_receipt=1, "
                        "updated_real_at=datetime('now') WHERE id=?",
                        (sid,),
                    )
                    conn.execute(
                        "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, detail) "
                        "VALUES(?,?,?, 'RECEIPT_LOST', 'cluster committed job but response lost')",
                        (sid, idem, now),
                    )
                    self.store.add_event(
                        conn,
                        sim_ts=now,
                        level="danger",
                        repo_id=rid,
                        message=(
                            f"第 {s['seq']} 班的创建请求：集群侧 Job 已创建并提交，"
                            f"但回执丢失，调度端未拿到 Job ID；可凭幂等键查回"
                        ),
                    )
                    summary["receipt_lost"].append(
                        {"shift_id": sid, "repo_id": rid, "idempotency_key": exc.idempotency_key}
                    )
                    continue
                except RepoOccupied as exc:
                    # 并发下“实际创建时的占用”推翻了前面的预检查
                    conn.execute(
                        "UPDATE shifts SET blocked_by_job_id=?, "
                        "updated_real_at=datetime('now') WHERE id=?",
                        (exc.running_job_id, sid),
                    )
                    conn.execute(
                        "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, detail) "
                        "VALUES(?,?,?, 'OCCUPIED', ?)",
                        (sid, idem, now, f"occupied by {exc.running_job_id} at insert"),
                    )
                    summary["blocked"].append(
                        {
                            "shift_id": sid,
                            "repo_id": rid,
                            "blocked_by_job_id": exc.running_job_id,
                        }
                    )
                    continue

                source = "IDEMPOTENT_RETRY" if job.get("replayed") else "RECEIPT"
                conn.execute(
                    "UPDATE shifts SET status='CREATED', cluster_job_id=?, job_source=?, "
                    "pending_lost_receipt=0, blocked_by_job_id=NULL, "
                    "updated_real_at=datetime('now') WHERE id=?",
                    (job["job_id"], source, sid),
                )
                conn.execute(
                    "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, "
                    "cluster_job_id, detail) VALUES(?,?,?, 'RECEIVED', ?, ?)",
                    (sid, idem, now, job["job_id"], source),
                )
                self.store.add_event(
                    conn,
                    sim_ts=now,
                    level="success",
                    repo_id=rid,
                    message=f"第 {s['seq']} 班已在集群创建 Job {job['job_id']}（{JOB_SOURCE_TEXT[source]}）",
                )
                summary["created"].append(
                    {"shift_id": sid, "repo_id": rid, "job_id": job["job_id"], "source": source}
                )

            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

        summary["state"] = self.state()
        return summary

    # ------------------------------------------------------------- lookup
    def _lookup_shift_locked(
        self, conn: sqlite3.Connection, row: sqlite3.Row, now: int
    ) -> dict[str, Any]:
        s = {k: row[k] for k in row.keys()}
        job = self.cluster.get_job_by_idem(s["idem_key"])
        if job is None:
            self.store.add_event(
                conn,
                sim_ts=now,
                level="warn",
                repo_id=s["repo_id"],
                message=(
                    f"第 {s['seq']} 班按幂等键 {s['idem_key']} 查询集群：未找到 Job"
                ),
            )
            return {"shift_id": s["id"], "found": False}

        was_skipped = s["status"] == "SKIPPED"
        conn.execute(
            "UPDATE shifts SET status='CREATED', cluster_job_id=?, job_source='LOOKUP', "
            "pending_lost_receipt=0, blocked_by_job_id=NULL, "
            "recovered_after_skip=?, updated_real_at=datetime('now') WHERE id=?",
            (job["job_id"], 1 if was_skipped else s["recovered_after_skip"], s["id"]),
        )
        conn.execute(
            "INSERT INTO attempts(shift_id, idem_key, sim_ts, outcome, cluster_job_id, detail) "
            "VALUES(?,?,?, 'RECOVERED', ?, 'lookup cluster by idempotency key')",
            (s["id"], s["idem_key"], now, job["job_id"]),
        )
        extra = "（该班此前已被记为跳过，查回后改记为已建）" if was_skipped else ""
        self.store.add_event(
            conn,
            sim_ts=now,
            level="success",
            repo_id=s["repo_id"],
            message=(
                f"第 {s['seq']} 班已从集群查回原始 Job {job['job_id']}{extra}；"
                f"集群实际创建时间 sim_ts={job['created_ts']}"
            ),
        )
        return {"shift_id": s["id"], "found": True, "job_id": job["job_id"], "was_skipped": was_skipped}

    def lookup_shift(self, shift_id: int) -> dict[str, Any]:
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM shifts WHERE id=?", (int(shift_id),)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise NotFound(f"班次 {shift_id} 不存在")
            now = int(
                conn.execute(
                    "SELECT value FROM meta WHERE key='sim_now_ts'"
                ).fetchone()["value"]
            )
            result = self._lookup_shift_locked(conn, row, now)
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        result["state"] = self.state()
        return result

    def reconcile_lost_receipts(self) -> dict[str, Any]:
        """一键查回所有 pending_lost_receipt=1 的班次（含已跳过的）。"""
        recovered: list[dict[str, Any]] = []
        not_found: list[int] = []
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = int(
                conn.execute(
                    "SELECT value FROM meta WHERE key='sim_now_ts'"
                ).fetchone()["value"]
            )
            rows = conn.execute(
                "SELECT * FROM shifts WHERE pending_lost_receipt=1 ORDER BY id ASC"
            ).fetchall()
            for row in rows:
                r = self._lookup_shift_locked(conn, row, now)
                if r["found"]:
                    recovered.append(r)
                else:
                    not_found.append(r["shift_id"])
            if rows:
                self.store.add_event(
                    conn,
                    sim_ts=now,
                    level="success" if recovered else "warn",
                    message=(
                        f"批量查回完成：{len(recovered)} 个班找回集群原 Job，"
                        f"{len(not_found)} 个未找到"
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        return {"recovered": recovered, "not_found": not_found, "state": self.state()}

    # ------------------------------------------------------------- cleanup
    def _shift_is_closed(self, s: sqlite3.Row) -> bool:
        """结案判定（只读集群库）。

        结案 = 已关联且集群 Job 已结束，或已跳过且集群没有同班 Job。
        待调度 / 仍在运行 / 有丢失回执待查的班次一律不算结案。
        判定是单调的：班次一旦结案，之后不会再变回未结案
        （Job 结束不可逆；已跳过的班不会再被调度，集群不会再生出同班 Job）。
        """
        if s["pending_lost_receipt"]:
            return False
        if s["status"] == "CREATED":
            jid = s["cluster_job_id"]
            if not jid:
                return False
            job = self.cluster.get_job(jid)
            return job is not None and job["status"] != "RUNNING"
        if s["status"] == "SKIPPED":
            return self.cluster.get_job_by_idem(s["idem_key"]) is None
        return False

    def cleanup_closed_shifts(self, repo_id: str, keep: int) -> dict[str, Any]:
        """清理某备份库的结案班次：按到点时间保留最新 keep 班，删除其余结案
        班次及其调度尝试；集群 Job 与事件流不动。

        - 待调度 / 仍在运行 / 回执待查的班次一律保留，且不占 keep 名额；
        - 整个清理在控制库单个 BEGIN IMMEDIATE 事务内完成，与调度/查回
          （同一把写锁）串行化，交错时活跃班次照常处理；
        - 任何一步失败都整体回滚，原始数据不变；
        - 删除后把该库清理水位线推进到被删班次的最大到点时间，
          重启/推进时钟/再次调度都不会重建已删班次。
        """
        try:
            keep = int(keep)
        except (TypeError, ValueError):
            raise ValidationError("保留班次数必须是整数")
        if keep < 0:
            raise ValidationError("保留班次数不能为负数（可填 0）")
        if keep > 100000:
            raise ValidationError("保留班次数过大")

        repos = {r["id"] for r in self.store.list_repos()}
        if repo_id not in repos:
            raise NotFound(f"未知备份库 {repo_id}")

        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = int(
                conn.execute(
                    "SELECT value FROM meta WHERE key='sim_now_ts'"
                ).fetchone()["value"]
            )
            rows = conn.execute(
                "SELECT * FROM shifts WHERE repo_id=? ORDER BY due_ts ASC, id ASC",
                (repo_id,),
            ).fetchall()
            closed = [r for r in rows if self._shift_is_closed(r)]
            n_delete = max(0, len(closed) - keep)
            to_delete = closed[:n_delete]
            kept_closed = closed[n_delete:]
            deleted_seqs = [int(r["seq"]) for r in to_delete]

            if to_delete:
                ids = [int(r["id"]) for r in to_delete]
                ph = ",".join("?" for _ in ids)
                conn.execute(f"DELETE FROM attempts WHERE shift_id IN ({ph})", ids)
                conn.execute(f"DELETE FROM shifts WHERE id IN ({ph})", ids)
                horizon = max(int(r["due_ts"]) for r in to_delete)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value "
                    "WHERE CAST(excluded.value AS INTEGER) > CAST(meta.value AS INTEGER)",
                    (self._horizon_key(repo_id), str(horizon)),
                )
            self.store.add_event(
                conn,
                sim_ts=now,
                level="info",
                repo_id=repo_id,
                message=(
                    f"清理结案班次：删除 {len(to_delete)} 班"
                    + (f"（班次号 {deleted_seqs}）" if to_delete else "")
                    + f"，按到点时间保留最新 {keep} 班结案班次；"
                      "待调度/运行中/回执待查班次未动，集群 Job 与事件流保留"
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

        return {
            "repo_id": repo_id,
            "keep": keep,
            "deleted": len(to_delete),
            "deleted_seqs": deleted_seqs,
            "kept_closed": len(kept_closed),
            "state": self.state(),
        }

    # ------------------------------------------------------------------ jobs
    def finish_job(self, job_id: str, result: str = "SUCCEEDED") -> dict[str, Any]:
        if result not in ("SUCCEEDED", "FAILED"):
            raise ValidationError("result 只能是 SUCCEEDED 或 FAILED")
        before = self.cluster.get_job(job_id)
        if before is None:
            raise NotFound(f"集群 Job {job_id} 不存在")

        now = self.store.get_sim_now()
        job = self.cluster.finish_job(job_id, now, result)

        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 引用该 Job 作为“前班占用”的等待班次解除引用
            conn.execute(
                "UPDATE shifts SET blocked_by_job_id=NULL "
                "WHERE blocked_by_job_id=?",
                (job_id,),
            )
            level = "success" if result == "SUCCEEDED" else "warn"
            self.store.add_event(
                conn,
                sim_ts=now,
                level=level,
                repo_id=job["repo_id"] if job else None,
                message=f"值班员结束 Job {job_id}，集群状态置为 {result}；同库后续班次现在可以补建",
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
        return {"job": job, "state": self.state()}

    # ------------------------------------------------------------------ reset
    def reset(self) -> dict[str, Any]:
        """删除两个 SQLite 库并重建初始演练（主要给反复演练/自动化测试用）。"""
        for p in (
            Path(self.store.path),
            Path(self.store.path + "-wal"),
            Path(self.store.path + "-shm"),
            Path(self.cluster.path),
            Path(self.cluster.path + "-wal"),
            Path(self.cluster.path + "-shm"),
        ):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        self.store.init_schema()
        self.cluster.init_schema()
        self._ensure_all_shifts()
        now = self.store.get_sim_now()
        conn = self.store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self.store.add_event(
                conn, sim_ts=now, level="info", message="演练环境已重置，模拟时钟回到初始锚点"
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
        return self.state()

    # ------------------------------------------------------------------ state
    def state(self) -> dict[str, Any]:
        now = self.store.get_sim_now()
        repo_rows = self.store.list_repos()
        repos: list[dict[str, Any]] = []
        for r in repo_rows:
            d = {"id": r["id"], "name": r["name"]}
            d["fault_armed"] = self.cluster.is_lost_receipt_armed(r["id"])
            running = self.cluster.find_running(r["id"])
            d["running_job_id"] = running["job_id"] if running else None
            repos.append(d)

        conn = self.store.connect()
        try:
            shift_rows = conn.execute(
                "SELECT * FROM shifts ORDER BY due_ts DESC, repo_id ASC, id DESC LIMIT 300"
            ).fetchall()
            shifts_raw = [dict(r) for r in shift_rows]
        finally:
            conn.close()

        jobs = self.cluster.list_jobs(1000)
        jobs_by_id = {j["job_id"]: j for j in jobs}

        shifts: list[dict[str, Any]] = []
        stats = {"created": 0, "waiting": 0, "skipped": 0, "upcoming": 0,
                 "running_jobs": 0, "lost_receipt": 0}
        for s in shifts_raw:
            due, deadline = s["due_ts"], s["deadline_ts"]
            linked = jobs_by_id.get(s["cluster_job_id"]) if s["cluster_job_id"] else None
            blocked_job = jobs_by_id.get(s["blocked_by_job_id"]) if s["blocked_by_job_id"] else None

            if due > now:
                bucket = "upcoming"
                stats["upcoming"] += 1
            elif s["status"] == "CREATED":
                bucket = "created"
                stats["created"] += 1
            elif s["status"] == "SKIPPED":
                bucket = "skipped"
                stats["skipped"] += 1
            else:
                stats["waiting"] += 1
                if now >= deadline:
                    bucket = "expired_pending"
                elif blocked_job is not None and blocked_job.get("status") == "RUNNING":
                    bucket = "blocked"
                else:
                    bucket = "ready"

            if s["pending_lost_receipt"]:
                stats["lost_receipt"] += 1

            shifts.append({
                **s,
                "bucket": bucket,
                "seconds_to_deadline": deadline - now,
                "window_length_sec": MAKEUP_WINDOW_SEC,
                "linked_job": linked,
                "blocked_job": blocked_job,
                "skip_reason_text": SKIP_REASON_TEXT.get(s["skip_reason"], s["skip_reason"]),
                "job_source_text": JOB_SOURCE_TEXT.get(s["job_source"], s["job_source"]),
                "can_lookup": s["status"] != "CREATED" or linked is None,
            })

        running_jobs = [j for j in jobs if j["status"] == "RUNNING"]
        stats["running_jobs"] = len(running_jobs)

        return {
            "ok": True,
            "server_time_real": time.strftime("%Y-%m-%d %H:%M:%S"),
            "sim_now_ts": now,
            "interval_sec": SHIFT_INTERVAL_SEC,
            "window_sec": MAKEUP_WINDOW_SEC,
            "anchor_ts": ANCHOR_TS,
            "stats": stats,
            "repos": repos,
            "shifts": shifts,
            "running_jobs": running_jobs,
            "recent_jobs": jobs[:30],
            "events": self.store.list_events(50),
        }
