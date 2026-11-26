# 备份校验调度演练台（Flask + 双 SQLite 库 + Docker Compose）

一个可反复演练“备份校验每 10 分钟一班、到点后 3 分钟内补建 Job、前班运行中本班等待、
满 3 分钟跳过、回执丢失后仍可从集群查回原 Job、并发调度不重不串”的交互演练台。

- 值班员可在页面上：**推进模拟时钟 → 触发调度 → 结束 Job**，并可布置一次性“丢回执”故障、
  按班次/一键从集群查回 Job。
- **演练控制库 `drill.db`** 保存模拟时钟、班次状态、调度尝试、事件流。
- **模拟集群库 `cluster.db`** 独立保存真正的 Job，带幂等唯一键和
  “同一备份库至多一个 RUNNING”的部分唯一索引。
- 两个库都在 `DATA_DIR`（容器内 `/data`），Compose 用命名卷持久化，重启后接着演练。

---

## 1. 快速开始

### 1.1 推荐：Docker Compose

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

- 页面：<http://localhost:8000>
- 健康检查：<http://localhost:8000/api/health>
- 数据持久化在 Compose 命名卷 `drill-data`（映射容器 `/data`，内含 `drill.db`、`cluster.db`）。

```bash
docker compose restart        # 重启：时钟/班次/Job 全保留
docker compose down           # 停止但保留数据卷，再 up 仍可接着练
docker compose down -v        # 连数据卷一起删除（恢复全新演练）
```

### 1.2 本地直接运行（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 方式 A：Flask 自带服务器
python -m app.web             # http://127.0.0.1:8000

# 方式 B：与 Docker 完全一致的多 worker 启动方式
DATA_DIR=./data gunicorn --preload --workers 4 \
  --bind 127.0.0.1:8000 app.web:app
```

不设 `DATA_DIR` 时，两个 SQLite 库默认落在仓库根目录 `./data/`（已被 `.gitignore` 忽略）。

### 1.3 自动化核验脚本

```bash
# 核心状态机/并发/重启单测（只用标准库 + 本仓库代码）
PYTHONPATH=. python3 -m unittest tests.test_drill -v

# 对已启动服务做 18 项 HTTP 端到端核验（含 12 并发、丢回执、超期、查回）
pip install requests
BASE_URL=http://127.0.0.1:8000 DATA_DIR=./data python3 scripts/e2e_check.py
```

---

## 2. 需求 → 实现对照

| 题目要求 | 实现方式 |
| --- | --- |
| 每 10 分钟一班，到点起 3 分钟可补建 | 固定网格 `due_ts = 锚点 + k*600`，`deadline_ts = due_ts + 180`；`now >= deadline` 即“满 3 分钟”判跳过，`deadline-1s` 仍可建（有边界单测） |
| 推进时钟 / 触发调度 / 结束 Job | 页面按钮 + JSON API：`/api/clock/advance`、`/api/scheduler/run`、`/api/jobs/<id>/finish` |
| 模拟集群用 SQLite **独立**保存 Job | 独立文件 `cluster.db` 与独立模块 `app/cluster.py`；控制面 `drill.db` 只存“关联结果”，不是 Job 真相源 |
| 创建成功后丢一次回执 | 每个备份库一个一次性开关，故障标记在集群创建事务内与 Job 插入**同一事务原子消费**，恰好丢一次；抛 `ReceiptLost`，且异常刻意不带 Job ID |
| 前班运行中，本班等它结束再补 | 调度时查集群同库 `RUNNING` Job → 本班保持 `WAITING` 并记录 `blocked_by_job_id`；结束前班后再次触发，窗口内即补建 |
| 满 3 分钟就跳过 | 到截止后触发调度 → `SKIPPED`，原因细分为 `BUSY_TIMEOUT`（前班占用到期）/ `WINDOW_EXPIRED`（无人占用但超期）/ `LOST_RECEIPT_EXPIRED`（回执丢失且补跑到期） |
| 每班最多一个 Job | 每班一个确定性幂等键 `backup-verify:<repo>:<due_ts>`，集群库 `idempotency_key UNIQUE`；窗口内重试走幂等重放，返回原 Job |
| 同一备份库不能同时两班运行 | 集群库部分唯一索引 `ux_jobs_one_running_per_repo ON jobs(repo_id) WHERE status='RUNNING'`，加 `BEGIN IMMEDIATE` 内“查占用→插入”，索引做并发兜底 |
| 并发调度以实际创建时的时间和占用为准 | 调度轮次在控制库 `BEGIN IMMEDIATE` 内**重读模拟时钟**并串行化；最终时间用该值写入 Job，占用以集群创建事务/唯一索引在 INSERT 瞬间裁决（有 24 线程调度 + 20 线程抢占用测试） |
| 重启后接着演练 | 模拟时钟、班次、事件、故障开关、Job 全部落盘；两个 Engine 均为“每次操作新开短连接”，多 worker/重启无内存状态 |
| 页面按班次显示等待/跳过/已建 | 班次表展示：未到点 / 等待补建 / 等待前班 / 窗口已过待判定 / 已建 / 已跳过，以及回执丢失、查回来源、倒计时、Job 状态与操作 |
| 回执丢了、补跑到期，仍能查回原 Job | 任意时刻可 `POST /api/shifts/<id>/lookup` 或 `/api/reconcile`，按幂等键直查 `cluster.db`；查到后班次转 `CREATED / LOOKUP`，并保留“曾跳过、超期后查回”标记与事件 |

预置了两个备份库 `repo-alpha`、`repo-beta`：同库互斥，不同库的 Job 可以同时运行。

---

## 3. 状态机与关键时序

班次在控制库只有三态：`WAITING / CREATED / SKIPPED`，页面再派生出更细的展示桶
（`ready / blocked / expired_pending / created / skipped / upcoming`）。

一轮 `run_scheduling(repo_id?)`（`app/engine.py`）：

1. 控制库 `BEGIN IMMEDIATE`，在事务内重读 `sim_now_ts`（推进时钟也要拿同一把写锁，
   因此不存在“判定后时钟被别的请求改掉”的窗口），并补齐网格班次。
2. 取所有 `due_ts <= now` 且 `WAITING` 的班次，按库、到点时间升序处理。
3. `now >= deadline_ts` → 记 `SKIPPED`（保留 `pending_lost_receipt`，不影响事后查回）。
4. 先按幂等键查集群：若已有 Job（丢回执后的窗口内重试）→ 直接关联为
   `CREATED / IDEMPOTENT_RETRY`，不重复建。
5. 查同库 `RUNNING`：有 → 记 `blocked`，本班继续等待。
6. 调 `ClusterDB.create_job()`：集群库 `BEGIN IMMEDIATE` 内再查幂等键、查占用、INSERT，
   并靠两条 UNIQUE/部分唯一索引兜底；返回正常回执，或在提交后抛一次性 `ReceiptLost`，
   或在 INSERT 撞占用时抛 `RepoOccupied`（调度端据此回到等待）。
7. 收到回执才把班次置 `CREATED` 并写 `cluster_job_id`；丢回执只置 `pending_lost_receipt=1`。

> 设计上“推进时钟”不会偷偷触发调度，必须由值班员显式点“触发调度”。
> 这样每一步因果都可在演练中看清，也方便制造“超期未触发 → 再触发判跳过”的场景。
> 页面每 5 秒静默刷新一次。

### 丢回执 + 超期 + 查回的完整故事

```
窗口内触发 ──► cluster.db INSERT Job 并 COMMIT
             同事务消费一次性故障开关
             调度端收到 ReceiptLost（拿不到 job_id），班次仍 WAITING/pending=1
推进时钟 > 3 分钟并触发 ──► 班次 SKIPPED/LOST_RECEIPT_EXPIRED（pending 仍保留）
点“查回集群 Job” ──► SELECT ... WHERE idempotency_key=? 命中原始行
                  班次改 CREATED/job_source=LOOKUP/recovered_after_skip=1
```

即使控制库的“跳过”判定已经发生，集群里的原始 Job 行与全部审计事件都不会被删除。

---

## 4. HTTP API 一览

| 方法 & 路径 | 作用 |
| --- | --- |
| `GET /api/state` | 页面所需全部状态（时钟、库、班次派生状态、运行 Job、事件、统计） |
| `GET /api/health` | 健康检查 |
| `GET /api/cluster/jobs?repo_id=` | **直接读集群库**，核对集群原始 Job |
| `POST /api/clock/advance` | body `{"seconds": 60}`，只允许正整数（≤86400） |
| `POST /api/scheduler/run` | body 可选 `{"repo_id":"repo-alpha"}`；不传 = 全部库 |
| `POST /api/jobs/<job_id>/finish` | body 可选 `{"result":"SUCCEEDED"}` |
| `POST /api/faults/lost-receipt` | `{"repo_id":"...","armed":true}`，一次性故障布置/取消 |
| `POST /api/shifts/<id>/lookup` | 按幂等键从集群查回单个班的原 Job |
| `POST /api/reconcile` | 一键查回所有 `pending_lost_receipt=1` 的班 |
| `POST /api/drill/reset` | 删除并重建两个 SQLite 库，回到 10:00 初始演练 |

### 用 curl 走一遍两条关键路径

```bash
B=http://127.0.0.1:8000
curl -X POST $B/api/drill/reset

# 路径 A：前班占用 → 超期跳过
curl -X POST $B/api/scheduler/run -H 'Content-Type: application/json' -d '{"repo_id":"repo-alpha"}'
# 从 /api/state 取第 1 班 cluster_job_id 记为 J1
curl -X POST $B/api/clock/advance -H 'Content-Type: application/json' -d '{"seconds":600}'
curl -X POST $B/api/scheduler/run  -H 'Content-Type: application/json' -d '{"repo_id":"repo-alpha"}'
curl -X POST $B/api/clock/advance -H 'Content-Type: application/json' -d '{"seconds":240}'
curl -X POST $B/api/scheduler/run  -H 'Content-Type: application/json' -d '{"repo_id":"repo-alpha"}'
# 第 2 班此时 SKIPPED/BUSY_TIMEOUT；结束 J1 后再推进可继续后面的班

# 路径 B：丢回执 → 补跑到期 → 查回原 Job
# 先结束路径 A 里仍在运行的 J1（用上面 /api/state 查到的 cluster_job_id）
curl -X POST $B/api/jobs/<J1_ID>/finish -H 'Content-Type: application/json' -d '{}'
curl -X POST $B/api/clock/advance -H 'Content-Type: application/json' -d '{"seconds":360}'  # 到 10:20
curl -X POST $B/api/faults/lost-receipt -H 'Content-Type: application/json' \
     -d '{"repo_id":"repo-alpha","armed":true}'
curl -X POST $B/api/scheduler/run -H 'Content-Type: application/json' -d '{"repo_id":"repo-alpha"}'
curl -X POST $B/api/clock/advance -H 'Content-Type: application/json' -d '{"seconds":240}'
curl -X POST $B/api/scheduler/run -H 'Content-Type: application/json' -d '{"repo_id":"repo-alpha"}'
# 从 /api/state 取第 3 班 id：SKIPPED + LOST_RECEIPT_EXPIRED + pending_lost_receipt=1
curl -X POST $B/api/shifts/<SHIFT_DB_ID>/lookup
curl "$B/api/cluster/jobs?repo_id=repo-alpha"   # 集群原始行始终都在
```

---

## 5. 目录结构

```
/workspace
├── Dockerfile                 # python:3.11-slim + gunicorn --preload -w4，非 root，HEALTHCHECK
├── docker-compose.yml         # web:8000 + 命名卷 drill-data:/data
├── requirements.txt           # Flask 3.x、gunicorn 23+
├── app/
│   ├── config.py              # 600s/180s 规则、锚点 2026-11-26 10:00 +08、DATA_DIR
│   ├── timeutil.py            # UTC+8 模拟时间格式化
│   ├── cluster.py             # 模拟集群 cluster.db：Job CRUD、占用约束、一次性丢回执
│   ├── store.py               # 控制库 drill.db schema/连接/事件
│   ├── engine.py              # 班次状态机、调度轮、查回、重置、state 组装
│   ├── web.py                 # Flask 工厂 + 页面 + JSON API + 错误处理
│   ├── templates/index.html   # 演练台页面
│   └── static/{styles.css,app.js}
├── tests/test_drill.py        # 12 项单测/并发竞争/重启测试
└── scripts/e2e_check.py       # 18 项 HTTP 端到端核验
```

---

## 6. 实测结果（交付前已执行）

实测环境：本沙箱 Debian、Python 3.11.2、SQLite 3.40.1、Node v22.23.4；
宿主机原本没有 pip/Flask，已在独立目录安装 Flask 3.1.3、gunicorn 27.0.1、requests 用于实测。

### 6.1 核心测试：12/12 通过

```
$ PYTHONPATH=/tmp/pylibs:/workspace python3 -m unittest tests.test_drill -v
test_shifts_generated_and_clock_persists ... ok
test_happy_path_one_job_per_shift ... ok
test_different_repos_can_run_in_parallel ... ok
test_wait_previous_then_create_within_window ... ok
test_create_allowed_one_second_before_deadline ... ok
test_busy_until_full_3min_then_skip ... ok
test_idle_trigger_after_deadline_skips_window_expired ... ok
test_lost_then_idempotent_retry_within_window ... ok
test_lost_expire_then_lookup_after_restart ... ok
test_parallel_scheduler_same_shift_creates_one_job ... ok      # 24 线程并发同一轮调度
test_cluster_occupancy_hard_constraint_race ... ok             # 20 线程抢同库 RUNNING 名额
test_same_idempotency_key_race_single_row ... ok
----------------------------------------------------------------------
Ran 12 tests in 1.796s — OK
```

其中：20 个不同幂等键并发建同库 Job，恰好 1 个成功、19 个收到 `RepoOccupied`；
12 个同幂等键并发只产生 1 行 Job；丢回执 + 超期 + 查回后**新建第二个 Engine
指向同一批文件**，时钟（`T0+240`）与 `CREATED/LOOKUP` 状态都在。

### 6.2 HTTP 端到端：18/18 通过（gunicorn `--preload --workers 4`）

按 Docker 的同一 WSGI 入口和多 worker 模型在 `127.0.0.1:8765` 实跑
（全新 `DATA_DIR=/tmp/drill-e2e2`），关键输出：

```
[PASS] 12 个并发调度请求全部成功 -- ok=12/12
[PASS] 并发后 beta 第 1 班只建出 1 个 Job -- jobs=['job-c9109bd55b6c']
[PASS] alpha 10:00 班正常创建 -- job-0813873e4659
[PASS] 10:10 班等待前班 -- bucket=blocked blocked=job-0813873e4659
[PASS] 前班一直运行、满 3 分钟后本班跳过 -- SKIPPED/BUSY_TIMEOUT
[PASS] 调度端收到一次回执丢失
[PASS] 丢回执后班次保持 WAITING 且 pending=1
[PASS] 集群侧实际已创建出运行中的 Job（调度端不知道 ID） -- job-4b86be442476
[PASS] 丢回执故障仅生效一次后自动撤防
[PASS] 补跑也到期后标记跳过且保留丢回执线索 -- SKIPPED/LOST_RECEIPT_EXPIRED/pending=1
[PASS] 查回 ... job_id == 集群原 Job job-4b86be442476, was_skipped=True
[PASS] 查回后 ... CREATED / LOOKUP / recovered_after_skip=1
[PASS] 直接读 cluster.db 能查到原 Job 行
       -- ('job-4b86be442476','RUNNING','backup-verify:repo-alpha:1795659600')
[PASS] 10:30 班被查回的运行中 Job 挡住
[PASS] 前班结束后在 3 分钟窗口内补建成功 -- job-d0e04d130471
共 18 项检查，失败 0 项 — ALL E2E CHECKS PASSED
```

### 6.3 重启后接着演练：通过

对 4-worker gunicorn 发 SIGTERM 停服、用**同一 DATA_DIR** 重新启动后：

```
[PASS] 重启后时钟/跳过班/查回班全部保留: 1795660320 {... 'created':4,'skipped':1 ...}
[PASS] 重启后可继续推进时钟并正确调度（新班被运行中前班挡住）
[PASS] cluster.db 中查回的原始 Job 重启后仍在，alpha 共 3 个 Job
```

### 6.4 页面渲染：jsdom 23 项断言通过

用 jsdom 加载真实 `index.html/app.js`，喂入真实 `/api/state` 以及人为构造的
“等待前班 / 窗口已过 / 回执丢失 / 超期跳过後查回 / 未到点”全分支状态：
时钟、2 张库卡片、班次表、集群 Job 表、事件流、丢失横幅、12 类关键文案、
筛选切换和全部按钮处理函数均无 JS 运行时错误。`node --check app.js` 也通过。

### 6.5 Docker 制品的核对

- `docker-compose.yml` 已用 PyYAML 解析：服务 `web` 构建 `.`、`8000:8000`、
  命名卷 `drill-data:/data`、`restart: unless-stopped`、healthcheck 齐全。
- Dockerfile 的 healthcheck 单行 Python 已在本机用等价 URL 实跑通过。
- 容器启动命令所用的 `app.web:app` 已用**完全相同的 gunicorn 参数**（含 `--preload -w4`）
  实测通过；容器内仅路径/端口不同（`DATA_DIR=/data`、绑定 `0.0.0.0:8000`）。

---

## 7. 余项与已知限制（如实说明）

1. **本沙箱没有安装/运行 Docker（也无 Podman/nerdctl，无 `/var/run/docker.sock`）**，
   因此我无法在这里字面执行 `docker compose up --build`。已做到的最大等价验证是：
   用相同的 Python 3.11 依赖与完全相同的 gunicorn 多 worker 启动行跑通 18 项 HTTP 核验，
   并静态校验 Compose YAML / healthcheck。交付到有 Docker 的环境后，
   `docker compose up --build` 应为一条命令成功；若构建有网络镜像问题可改用国内 pip 源。
2. 这是**演练/教学工具**：没有登录鉴权、没有多用户权限隔离，定位为本机或可信内网使用。
3. 时钟是**手动推进的模拟时钟**，没有挂真实定时器；这是刻意设计（保证演练步骤确定、
   可重复、可测试），不是缺陷。需要“自动播片”时可后续加一个按固定间隔调用 advance+run 的开关。
4. 并发正确性依赖 SQLite 写锁与两条约束，调度轮在控制库侧全局串行化；
   对演练规模绰绰有余，但不应外推为生产级分布式调度。
5. “重置演练”会删除两个 SQLite 文件（页面上有二次确认）；正常 `restart/down/up` 不删卷，
   只有 `docker compose down -v` 或手动调 reset 才会清空。
6. 未做真实浏览器截图像素级验收（环境无 Chromium），已用 jsdom 断言 + 静态资源 200 替代；
   CSS 为手写无外部 CDN，离线可用。
