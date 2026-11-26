"""核心状态机 / 集群约束 / 并发 / 重启持久化测试。

运行：
    PYTHONPATH=/workspace python3 -m unittest tests.test_drill -v
"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.cluster import ClusterDB, ReceiptLost, RepoOccupied  # noqa: E402
from app.config import ANCHOR_TS  # noqa: E402
from app.engine import Engine  # noqa: E402

T0 = ANCHOR_TS
T1 = T0 + 600
DEADLINE1 = T0 + 180
REPO = "repo-alpha"


class DrillTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        d = Path(self._tmp.name)
        self.eng = Engine(d / "drill.db", d / "cluster.db")
        self.cluster_path = d / "cluster.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def shift(self, seq: int, repo: str = REPO) -> dict:
        for s in self.eng.state()["shifts"]:
            if s["seq"] == seq and s["repo_id"] == repo:
                return s
        raise AssertionError(f"shift seq={seq} repo={repo} not found")

    def alpha_jobs(self) -> list[dict]:
        return [j for j in self.eng.cluster.list_jobs(500) if j["repo_id"] == REPO]


class TestBasics(DrillTestBase):
    def test_shifts_generated_and_clock_persists(self):
        st = self.eng.state()
        self.assertEqual(st["sim_now_ts"], T0)
        s1 = self.shift(1)
        self.assertEqual(s1["due_ts"], T0)
        self.assertEqual(s1["deadline_ts"], T0 + 180)
        self.assertEqual(s1["bucket"], "ready")
        # 未来班次是 upcoming
        self.assertEqual(self.shift(2)["bucket"], "upcoming")

        self.eng.advance_clock(60)
        self.assertEqual(self.eng.store.get_sim_now(), T0 + 60)

    def test_happy_path_one_job_per_shift(self):
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["created"]), 1)
        jid = out["created"][0]["job_id"]
        s1 = self.shift(1)
        self.assertEqual(s1["status"], "CREATED")
        self.assertEqual(s1["job_source"], "RECEIPT")
        self.assertEqual(s1["cluster_job_id"], jid)

        # 再触发一次：不得重复建 Job
        out2 = self.eng.run_scheduling(REPO)
        self.assertEqual(out2["created"], [])
        self.assertEqual(len(self.alpha_jobs()), 1)

    def test_different_repos_can_run_in_parallel(self):
        self.eng.run_scheduling("repo-alpha")
        self.eng.run_scheduling("repo-beta")
        running = self.eng.state()["running_jobs"]
        self.assertEqual(len(running), 2)
        repos = {j["repo_id"] for j in running}
        self.assertEqual(repos, {"repo-alpha", "repo-beta"})


class TestWaitingAndSkip(DrillTestBase):
    def _first_job_id(self) -> str:
        return self.shift(1)["cluster_job_id"]

    def test_wait_previous_then_create_within_window(self):
        self.eng.run_scheduling(REPO)  # 10:00 班 J0 运行中

        self.eng.advance_clock(600)   # 10:10 到点
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["blocked"]), 1)
        self.assertEqual(out["blocked"][0]["blocked_by_job_id"], self._first_job_id())
        self.assertEqual(self.shift(2)["bucket"], "blocked")
        self.assertEqual(len(self.alpha_jobs()), 1)  # 等待期间不建班

        self.eng.advance_clock(90)    # 10:11:30，距截止 10:13 还有 90s
        j0 = self._first_job_id()
        self.eng.finish_job(j0)
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["created"]), 1)
        self.assertEqual(out["created"][0]["source"], "RECEIPT")

        jobs = {j["job_id"]: j for j in self.alpha_jobs()}
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[j0]["status"], "SUCCEEDED")
        j1 = out["created"][0]["job_id"]
        self.assertEqual(jobs[j1]["status"], "RUNNING")
        # 实际创建时间以创建时读到的模拟时钟为准
        self.assertEqual(jobs[j1]["created_ts"], T0 + 690)

    def test_create_allowed_one_second_before_deadline(self):
        self.eng.run_scheduling(REPO)
        self.eng.advance_clock(179 + 600)  # T1+179 = deadline1 - 1s? no:
        # 当前 T0+779，正是第 2 班截止 T0+780 的前一秒
        self.assertEqual(self.eng.store.get_sim_now(), T0 + 779)
        self.eng.finish_job(self._first_job_id())
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["created"]), 1)

    def test_busy_until_full_3min_then_skip(self):
        self.eng.run_scheduling(REPO)
        self.eng.advance_clock(600)
        self.eng.run_scheduling(REPO)  #  blocked once

        self.eng.advance_clock(180)    # 恰好到第 2 班 deadline（满 3 分钟）
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["skipped"]), 1)
        self.assertEqual(out["skipped"][0]["reason"], "BUSY_TIMEOUT")
        s2 = self.shift(2)
        self.assertEqual(s2["status"], "SKIPPED")
        self.assertEqual(s2["skip_reason"], "BUSY_TIMEOUT")
        self.assertEqual(len(self.alpha_jobs()), 1)  # 跳过即不补建

    def test_idle_trigger_after_deadline_skips_window_expired(self):
        self.eng.advance_clock(180)    # 直接过第 1 班窗口
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(out["skipped"][0]["reason"], "WINDOW_EXPIRED")
        self.assertEqual(len(self.alpha_jobs()), 0)


class TestLostReceipt(DrillTestBase):
    def test_lost_then_idempotent_retry_within_window(self):
        self.eng.set_fault(REPO, True)
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out["receipt_lost"]), 1)

        s1 = self.shift(1)
        self.assertEqual(s1["status"], "WAITING")
        self.assertEqual(s1["pending_lost_receipt"], 1)
        self.assertIsNone(s1["cluster_job_id"])

        jobs = self.alpha_jobs()
        self.assertEqual(len(jobs), 1)  # 集群侧其实已建

        # 再触发：凭同一幂等键拿回原 Job，不重复创建
        out2 = self.eng.run_scheduling(REPO)
        self.assertEqual(len(out2["created"]), 1)
        self.assertEqual(out2["created"][0]["source"], "IDEMPOTENT_RETRY")
        self.assertEqual(out2["created"][0]["job_id"], jobs[0]["job_id"])
        self.assertEqual(len(self.alpha_jobs()), 1)
        self.assertFalse(self.eng.state()["repos"][0]["fault_armed"])

    def test_lost_expire_then_lookup_after_restart(self):
        self.eng.set_fault(REPO, True)
        self.eng.run_scheduling(REPO)
        original = self.alpha_jobs()[0]

        # 不做窗口内补跑，直接推过 3 分钟
        self.eng.advance_clock(240)
        out = self.eng.run_scheduling(REPO)
        self.assertEqual(out["skipped"][0]["reason"], "LOST_RECEIPT_EXPIRED")
        s1 = self.shift(1)
        self.assertEqual(s1["status"], "SKIPPED")
        self.assertEqual(s1["pending_lost_receipt"], 1)

        # 即使超期，仍可按幂等键查回集群原 Job
        res = self.eng.lookup_shift(s1["id"])
        self.assertTrue(res["found"])
        self.assertEqual(res["job_id"], original["job_id"])
        self.assertTrue(res["was_skipped"])
        s1b = self.shift(1)
        self.assertEqual(s1b["status"], "CREATED")
        self.assertEqual(s1b["job_source"], "LOOKUP")
        self.assertEqual(s1b["cluster_job_id"], original["job_id"])
        self.assertEqual(s1b["recovered_after_skip"], 1)
        self.assertEqual(len(self.alpha_jobs()), 1)

        # 重启后状态仍在，且可继续演练
        eng2 = Engine(
            Path(self._tmp.name) / "drill.db",
            Path(self._tmp.name) / "cluster.db",
        )
        self.assertEqual(eng2.store.get_sim_now(), T0 + 240)
        s1c = [s for s in eng2.state()["shifts"] if s["id"] == s1["id"]][0]
        self.assertEqual(s1c["status"], "CREATED")
        self.assertEqual(s1c["job_source"], "LOOKUP")
        # 查回接口幂等
        again = eng2.lookup_shift(s1["id"])
        self.assertTrue(again["found"])
        self.assertEqual(eng2.cluster.count_jobs(), 1)


class TestConcurrency(DrillTestBase):
    def test_parallel_scheduler_same_shift_creates_one_job(self):
        errors: list[Exception] = []

        def worker():
            try:
                self.eng.run_scheduling(REPO)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda _: worker(), range(24)))

        self.assertEqual(errors, [])
        self.assertEqual(len(self.alpha_jobs()), 1)
        self.assertEqual(self.shift(1)["status"], "CREATED")

    def test_cluster_occupancy_hard_constraint_race(self):
        cdb = ClusterDB(self.cluster_path)
        cdb.init_schema()
        successes, occupied = [], []
        lock = threading.Lock()

        def create(i: int):
            try:
                j = cdb.create_job(
                    idempotency_key=f"different-key-{i}",
                    repo_id=REPO,
                    shift_due_ts=T0,
                    sim_now_ts=T0,
                )
                with lock:
                    successes.append(j["job_id"])
            except RepoOccupied:
                with lock:
                    occupied.append(i)

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(create, range(20)))

        self.assertEqual(len(successes), 1, successes)
        self.assertEqual(len(occupied), 19)

        # 结束占用后，下一个创建成功
        cdb.finish_job(successes[0], T0 + 10)
        j2 = cdb.create_job(
            idempotency_key="next-shift-key", repo_id=REPO,
            shift_due_ts=T1, sim_now_ts=T0 + 10,
        )
        self.assertEqual(j2["status"], "RUNNING")

    def test_same_idempotency_key_race_single_row(self):
        cdb = ClusterDB(self.cluster_path)
        ids = set()
        lock = threading.Lock()

        def create(_i: int):
            try:
                j = cdb.create_job(
                    idempotency_key="same-key", repo_id="repo-beta",
                    shift_due_ts=T0, sim_now_ts=T0,
                )
                with lock:
                    ids.add(j["job_id"])
            except ReceiptLost:
                pass

        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(create, range(12)))

        rows = [j for j in cdb.list_jobs(100) if j["idempotency_key"] == "same-key"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(ids), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
