from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---- 调度规则（题目固定值） -------------------------------------------------
SHIFT_INTERVAL_SEC = 10 * 60       # 每 10 分钟一班
MAKEUP_WINDOW_SEC = 3 * 60         # 到点后 3 分钟补建窗口
# 允许的边界语义：now < due + 180 可建；now >= due + 180 记跳过（“满 3 分钟”）

LOOKAHEAD_SEC = 60 * 60            # 页面提前生成未来 1 小时的班次
MAX_ADVANCE_SEC = 24 * 3600        # 单次推进时钟上限，防止误填

# 演练起始模拟时间：2026-11-26 10:00:00 +08:00（恰好是 10 分钟网格点）
DISPLAY_TZ = timezone(timedelta(hours=8))
ANCHOR_DT = datetime(2026, 11, 26, 10, 0, 0, tzinfo=DISPLAY_TZ)
ANCHOR_TS = int(ANCHOR_DT.timestamp())

# ---- 路径：容器内 DATA_DIR=/data；本地默认 <仓库>/data -----------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR") or (_REPO_ROOT / "data"))
CONTROL_DB_PATH = DATA_DIR / "drill.db"
CLUSTER_DB_PATH = DATA_DIR / "cluster.db"

# 两个预置备份库，用于演示“同一备份库互斥、不同备份库可并行”
SEED_REPOS = [
    ("repo-alpha", "备份库 A · alpha"),
    ("repo-beta", "备份库 B · beta"),
]

SCHEMA_VERSION = 1
