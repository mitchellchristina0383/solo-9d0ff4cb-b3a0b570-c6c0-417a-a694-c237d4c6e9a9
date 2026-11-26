"""Flask 页面与 JSON API。

gunicorn 入口：app.web:app（模块底部按环境变量 DATA_DIR 创建）。
也可用 create_app(data_dir=...) 在测试中指向临时目录。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from .engine import Engine, NotFound, ValidationError


def create_app(data_dir: str | Path | None = None) -> Flask:
    app = Flask(__name__)

    if data_dir is not None:
        data_dir = Path(data_dir)
        engine = Engine(data_dir / "drill.db", data_dir / "cluster.db")
    else:
        # 默认路径在 config 中按 DATA_DIR 环境变量解析（容器内为 /data）
        engine = Engine()
    app.extensions["drill_engine"] = engine

    def eng() -> Engine:
        return app.extensions["drill_engine"]

    # ---------------------------------------------------------------- pages
    @app.get("/")
    def index() -> str:
        return render_template("index.html")

    @app.get("/api/health")
    def health() -> Any:
        e = eng()
        now = e.store.get_sim_now()
        return jsonify({"ok": True, "sim_now_ts": now})

    @app.get("/api/state")
    def get_state() -> Any:
        return jsonify(eng().state())

    @app.get("/api/cluster/jobs")
    def cluster_jobs() -> Any:
        """直接读模拟集群库，便于核对“集群里到底有哪些 Job”。"""
        repo_id = request.args.get("repo_id")
        jobs = eng().cluster.list_jobs(1000)
        if repo_id:
            jobs = [j for j in jobs if j["repo_id"] == repo_id]
        return jsonify({"ok": True, "jobs": jobs})

    # ---------------------------------------------------------------- actions
    @app.post("/api/clock/advance")
    def clock_advance() -> Any:
        body = request.get_json(silent=True) or {}
        state = eng().advance_clock(body.get("seconds"))
        return jsonify({"ok": True, "state": state})

    @app.post("/api/scheduler/run")
    def scheduler_run() -> Any:
        body = request.get_json(silent=True) or {}
        repo_id = body.get("repo_id")
        summary = eng().run_scheduling(repo_id=repo_id)
        return jsonify({"ok": True, "summary": summary, "state": summary["state"]})

    @app.post("/api/jobs/<job_id>/finish")
    def job_finish(job_id: str) -> Any:
        body = request.get_json(silent=True) or {}
        result = body.get("result", "SUCCEEDED")
        out = eng().finish_job(job_id, result=result)
        return jsonify({"ok": True, "job": out["job"], "state": out["state"]})

    @app.post("/api/faults/lost-receipt")
    def set_fault() -> Any:
        body = request.get_json(silent=True) or {}
        repo_id = body.get("repo_id")
        if not repo_id:
            raise ValidationError("repo_id 必填")
        state = eng().set_fault(repo_id, bool(body.get("armed", True)))
        return jsonify({"ok": True, "state": state})

    @app.post("/api/shifts/<int:shift_id>/lookup")
    def shift_lookup(shift_id: int) -> Any:
        out = eng().lookup_shift(shift_id)
        result = {k: v for k, v in out.items() if k != "state"}
        return jsonify({"ok": True, "result": result, "state": out["state"]})

    @app.post("/api/reconcile")
    def reconcile() -> Any:
        out = eng().reconcile_lost_receipts()
        return jsonify({"ok": True,
                        "result": {"recovered": out["recovered"], "not_found": out["not_found"]},
                        "state": out["state"]})

    @app.post("/api/drill/reset")
    def drill_reset() -> Any:
        state = eng().reset()
        return jsonify({"ok": True, "state": state})

    # ---------------------------------------------------------------- errors
    @app.errorhandler(ValidationError)
    def _validation(err: ValidationError) -> Any:
        return jsonify({"ok": False, "error": "validation_error", "message": str(err)}), 400

    @app.errorhandler(NotFound)
    def _not_found(err: NotFound) -> Any:
        return jsonify({"ok": False, "error": "not_found", "message": str(err)}), 404

    @app.errorhandler(Exception)
    def _unexpected(err: Exception) -> Any:
        app.logger.exception("unhandled error: %s", err)
        return jsonify({"ok": False, "error": type(err).__name__, "message": str(err)}), 500

    return app


# gunicorn app.web:app 使用；DATA_DIR 在 config 内读取
app = create_app()


if __name__ == "__main__":
    # 本地调试：python -m app.web
    app.run(host="127.0.0.1", port=8000, threaded=True)
