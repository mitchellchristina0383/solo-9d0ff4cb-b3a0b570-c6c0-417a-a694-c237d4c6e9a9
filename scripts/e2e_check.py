#!/usr/bin/env python3
"""对运行中的网页服务做端到端核验（含 HTTP 并发、丢回执、超期等待、查回）。

用法：
    BASE_URL=http://127.0.0.1:8765 python3 scripts/e2e_check.py
可选：
    DATA_DIR=/tmp/drill-e2e   用于直接只读核对 cluster.db 原始行
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
DATA_DIR = os.environ.get("DATA_DIR", "")
ALPHA = "repo-alpha"
BETA = "repo-beta"

failures: list[str] = []
checks = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global checks
    checks += 1
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        failures.append(name)


def wait_health(timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE}/api/health", timeout=2)
            if r.ok and r.json().get("ok"):
                print(f"health ok: {r.json()}")
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.4)
    raise SystemExit(f"service not ready at {BASE}: {last}")


def post(path: str, body: dict | None = None) -> dict:
    r = requests.post(f"{BASE}{path}", json=body or {}, timeout=20)
    data = r.json()
    if not r.ok or not data.get("ok"):
        raise AssertionError(f"POST {path} -> {r.status_code} {data}")
    return data


def state() -> dict:
    return requests.get(f"{BASE}/api/state", timeout=10).json()


def shift_by_seq(st: dict, seq: int, repo: str = ALPHA) -> dict:
    for s in st["shifts"]:
        if s["seq"] == seq and s["repo_id"] == repo:
            return s
    raise AssertionError(f"shift seq={seq} not found")


def cluster_jobs(repo: str) -> list[dict]:
    r = requests.get(f"{BASE}/api/cluster/jobs", params={"repo_id": repo}, timeout=10)
    return r.json()["jobs"]


def main() -> int:
    wait_health()

    # 页面可打开
    html = requests.get(f"{BASE}/", timeout=10).text
    check("首页 HTML 含标题", "备份校验调度演练台" in html)

    # 全新演练，保证脚本可重复执行
    post("/api/drill/reset")

    # ---------- 1) HTTP 并发：12 个请求同时调度 beta 同一班 ----------
    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(
            lambda _: requests.post(f"{BASE}/api/scheduler/run",
                                    json={"repo_id": BETA}, timeout=30).json(),
            range(12),
        ))
    ok_count = sum(1 for r in responses if r.get("ok"))
    beta_jobs = cluster_jobs(BETA)
    check("12 个并发调度请求全部成功", ok_count == 12, f"ok={ok_count}/12")
    check("并发后 beta 第 1 班只建出 1 个 Job", len(beta_jobs) == 1,
          f"jobs={[j['job_id'] for j in beta_jobs]}")
    st = state()
    check("beta 第1班状态 CREATED", shift_by_seq(st, 1, BETA)["status"] == "CREATED")

    # ---------- 2) alpha 正常建首班（前班） ----------
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    j0 = r["state"]["shifts"]
    s1 = shift_by_seq(r["state"], 1)
    check("alpha 10:00 班正常创建", s1["status"] == "CREATED" and s1["job_source"] == "RECEIPT",
          s1.get("cluster_job_id"))
    job0 = s1["cluster_job_id"]

    # ---------- 3) 10:10 班被运行中的前班挡住 ----------
    post("/api/clock/advance", {"seconds": 600})
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    s2 = shift_by_seq(r["state"], 2)
    check("10:10 班等待前班", s2["bucket"] == "blocked"
          and s2["blocked_by_job_id"] == job0,
          f"bucket={s2['bucket']} blocked={s2.get('blocked_by_job_id')}")

    # ---------- 4) 超期等待：再推 240s（超过 10:13 截止），触发 -> 跳过 ----------
    post("/api/clock/advance", {"seconds": 240})
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    s2 = shift_by_seq(r["state"], 2)
    check("前班一直运行、满 3 分钟后本班跳过",
          s2["status"] == "SKIPPED" and s2["skip_reason"] == "BUSY_TIMEOUT",
          f"status={s2['status']} reason={s2.get('skip_reason')}")
    check("跳过时没有给第2班建 Job", s2["cluster_job_id"] is None)

    # 结束前班，推进到 10:20
    post(f"/api/jobs/{job0}/finish", {"result": "SUCCEEDED"})
    post("/api/clock/advance", {"seconds": 360})

    # ---------- 5) 丢回执：布置一次性故障后调度 10:20 班 ----------
    post("/api/faults/lost-receipt", {"repo_id": ALPHA, "armed": True})
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    lost = r["summary"]["receipt_lost"]
    s3 = shift_by_seq(r["state"], 3)
    check("调度端收到一次回执丢失", len(lost) == 1, str(lost))
    check("丢回执后班次保持 WAITING 且 pending=1",
          s3["status"] == "WAITING" and s3["pending_lost_receipt"] == 1
          and s3["cluster_job_id"] is None)
    alpha_jobs = cluster_jobs(ALPHA)
    # 此时 alpha 有 J0(SUCCEEDED) + J3(RUNNING)
    running_alpha = [j for j in alpha_jobs if j["status"] == "RUNNING"]
    check("集群侧实际已创建出运行中的 Job（调度端不知道 ID）",
          len(running_alpha) == 1, running_alpha[0]["job_id"] if running_alpha else "none")
    original_id = running_alpha[0]["job_id"]

    # 故障必须是一次性的
    st = state()
    alpha_repo = next(x for x in st["repos"] if x["id"] == ALPHA)
    check("丢回执故障仅生效一次后自动撤防", alpha_repo["fault_armed"] is False)

    # ---------- 6) 不补跑，直接推过 10:23 窗口，触发 -> 跳过(回执丢失超期) ----------
    post("/api/clock/advance", {"seconds": 240})
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    s3 = shift_by_seq(r["state"], 3)
    check("补跑也到期后标记跳过且保留丢回执线索",
          s3["status"] == "SKIPPED"
          and s3["skip_reason"] == "LOST_RECEIPT_EXPIRED"
          and s3["pending_lost_receipt"] == 1,
          f"{s3['status']}/{s3.get('skip_reason')}/pending={s3['pending_lost_receipt']}")

    # ---------- 7) 超期后仍可查回集群原始 Job ----------
    r = post(f"/api/shifts/{s3['id']}/lookup")
    check("查回接口 found=True 且 Job ID 与集群原 Job 一致",
          r["result"]["found"] and r["result"]["job_id"] == original_id
          and r["result"]["was_skipped"] is True,
          f"lookup={r['result']} original={original_id}")
    s3 = shift_by_seq(r["state"], 3)
    check("查回后班次转为已建 / LOOKUP / recovered_after_skip=1",
          s3["status"] == "CREATED" and s3["job_source"] == "LOOKUP"
          and s3["cluster_job_id"] == original_id
          and s3["recovered_after_skip"] == 1)

    # 直接只读核对独立 cluster.db 中原始行仍在、且只有一个 seq3 Job
    if DATA_DIR:
        cpath = os.path.join(DATA_DIR, "cluster.db")
        uri = f"file:{cpath}?mode=ro"
        con = sqlite3.connect(uri, uri=True)
        row = con.execute(
            "SELECT job_id, status, idempotency_key FROM jobs WHERE idempotency_key=?",
            (f"backup-verify:{ALPHA}:{s3['due_ts']}",),
        ).fetchone()
        con.close()
        check("直接读 cluster.db 能查到原 Job 行",
              row is not None and row[0] == original_id, str(row))

    # ---------- 8) 正向“等前班结束再补”窗口内成功路径（10:30 班） ----------
    post("/api/clock/advance", {"seconds": 360})   # 到 10:30
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    s4 = shift_by_seq(r["state"], 4)
    check("10:30 班被查回的运行中 Job 挡住",
          s4["bucket"] == "blocked" and s4["blocked_by_job_id"] == original_id)
    post("/api/clock/advance", {"seconds": 120})   # 10:32 < 10:33 截止
    post(f"/api/jobs/{original_id}/finish")
    r = post("/api/scheduler/run", {"repo_id": ALPHA})
    s4 = shift_by_seq(r["state"], 4)
    check("前班结束后在 3 分钟窗口内补建成功",
          s4["status"] == "CREATED" and s4["job_source"] == "RECEIPT",
          s4.get("cluster_job_id"))

    # 终态统计
    st = state()
    print("\n===== 终态 =====")
    print("sim_now_ts:", st["sim_now_ts"], "stats:", st["stats"])
    print("alpha jobs:", [(j["job_id"], j["status"]) for j in cluster_jobs(ALPHA)])

    print(f"\n共 {checks} 项检查，失败 {len(failures)} 项")
    if failures:
        print("FAILED:", failures)
        return 1
    print("ALL E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
