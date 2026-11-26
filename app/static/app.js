/* 备份校验调度演练台前端：拉取 /api/state 渲染，所有写操作走 JSON API。 */
"use strict";

let STATE = null;
let FILTER = "all";
let TOAST_TIMER = null;

// ---------- 工具 ----------
function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}
function pad(n) { return String(n).padStart(2, "0"); }
// 服务端按 UTC+8 格式化，这里保持一致：把时间戳加 8 小时后取 UTC 字段
function cstDate(ts) { return new Date((Number(ts) + 8 * 3600) * 1000); }
function fmtDT(ts) {
  if (ts == null) return "—";
  const d = cstDate(ts);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
         `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
function fmtShort(ts) {
  if (ts == null) return "—";
  const d = cstDate(ts);
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
         `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
function fmtT(ts) {
  if (ts == null) return "--:--:--";
  const d = cstDate(ts);
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}
function countdown(sec) {
  const sign = sec < 0 ? "-" : "";
  sec = Math.abs(sec);
  return `${sign}${pad(Math.floor(sec / 60))}:${pad(sec % 60)}`;
}
function toast(msg, isError) {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = "toast" + (isError ? " error" : "");
  clearTimeout(TOAST_TIMER);
  TOAST_TIMER = setTimeout(() => el.classList.add("hidden"), 4200);
}

async function api(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  let data = null;
  try { data = await res.json(); } catch (e) { /* ignore */ }
  if (!res.ok || !data || data.ok === false) {
    const msg = (data && data.message) ? data.message : `请求失败 HTTP ${res.status}`;
    toast(msg, true);
    throw new Error(msg);
  }
  if (data.state) { STATE = data.state; render(); }
  return data;
}

async function loadState() {
  try {
    const res = await fetch("/api/state");
    STATE = await res.json();
    render();
  } catch (e) {
    toast("加载状态失败：" + e, true);
  }
}

// ---------- 操作（onclick 全局可见） ----------
window.advance = (sec) => api("/api/clock/advance", { seconds: sec }).catch(() => {});
window.advanceCustom = () => {
  const m = parseInt(document.getElementById("advance-min").value || "0", 10);
  if (!Number.isFinite(m) || m <= 0) return toast("请输入正整数分钟", true);
  return api("/api/clock/advance", { seconds: m * 60 }).catch(() => {});
};
window.runScheduling = (repoId) =>
  api("/api/scheduler/run", repoId ? { repo_id: repoId } : {}).catch(() => {});
window.finishJob = (jobId) =>
  api(`/api/jobs/${encodeURIComponent(jobId)}/finish`, { result: "SUCCEEDED" }).catch(() => {});
window.lookupShift = (sid) =>
  api(`/api/shifts/${sid}/lookup`, {}).catch(() => {});
window.reconcile = () => api("/api/reconcile", {}).catch(() => {});
window.setFilter = (f) => { FILTER = f; render(); };
window.toggleFault = (repoId, checked) =>
  api("/api/faults/lost-receipt", { repo_id: repoId, armed: checked }).catch(() => loadState());

window.resetDrill = async () => {
  if (!confirm("确定清空两个 SQLite 库并重置整个演练？此操作不可撤销。")) return;
  await api("/api/drill/reset", {});
  toast("演练已重置");
};

window.concurrentRuns = async (n) => {
  n = n || 10;
  toast(`并发发出 ${n} 个调度请求…`);
  const results = await Promise.allSettled(
    Array.from({ length: n }, () =>
      fetch("/api/scheduler/run", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
      }).then((r) => r.json())
    )
  );
  const ok = results.filter((r) => r.status === "fulfilled" && r.value && r.value.ok).length;
  toast(`并发调度完成：${ok}/${n} 个请求成功；请核对每班仍至多 1 个 Job`);
  await loadState();
};

// ---------- 渲染 ----------
function statusBadge(s) {
  if (s.due_ts > STATE.sim_now_ts && s.bucket === "upcoming")
    return `<span class="badge gray"><span class="dot"></span>未到点</span>`;
  switch (s.bucket) {
    case "created":
      return `<span class="badge green"><span class="dot"></span>已建 Job</span>`;
    case "skipped":
      return `<span class="badge red"><span class="dot"></span>已跳过</span>`;
    case "blocked":
      return `<span class="badge amber"><span class="dot"></span>等待前班结束</span>`;
    case "expired_pending":
      return `<span class="badge red"><span class="dot"></span>窗口已过 · 待触发判跳过</span>`;
    default:
      return `<span class="badge blue"><span class="dot"></span>等待补建（可触发）</span>`;
  }
}

function jobStatusBadge(job) {
  if (!job) return "";
  if (job.status === "RUNNING")
    return `<span class="badge blue running"><span class="dot"></span>RUNNING</span>`;
  if (job.status === "SUCCEEDED")
    return `<span class="badge green">SUCCEEDED</span>`;
  return `<span class="badge red">${esc(job.status)}</span>`;
}

function renderRepos() {
  const el = document.getElementById("repo-cards");
  el.innerHTML = STATE.repos.map((r) => `
    <div class="repo-card">
      <div class="repo-head">
        <div>
          <div class="repo-name">${esc(r.name)}</div>
          <div class="repo-id">${esc(r.id)}</div>
        </div>
        <button class="btn primary small" onclick="runScheduling('${esc(r.id)}')">调度该库</button>
      </div>
      <div class="repo-body">
        <div class="occ">
          ${r.running_job_id
            ? `当前占用：<span class="mono"><b>${esc(r.running_job_id)}</b></span>
               <span class="badge blue running"><span class="dot"></span>运行中</span>
               <button class="btn small danger" onclick="finishJob('${esc(r.running_job_id)}')">结束 Job</button>`
            : `<span style="color:var(--green)">● 集群侧当前无 RUNNING Job</span>`}
        </div>
        <div class="fault-row">
          <label class="switch">
            <input type="checkbox" ${r.fault_armed ? "checked" : ""}
              onchange="toggleFault('${esc(r.id)}', this.checked)">
            <span class="slider"></span>
          </label>
          <span class="fault-state">
            ${r.fault_armed
              ? '<b style="color:var(--red)">故障已布置：</b>下一次创建在集群成功后丢失回执（仅一次）'
              : "布置一次性故障：下次创建成功后丢失回执"}
          </span>
        </div>
      </div>
    </div>`).join("");
}

function renderTabs() {
  const tabs = [{ id: "all", name: "全部" }, ...STATE.repos.map((r) => ({ id: r.id, name: r.name }))];
  document.getElementById("filter-tabs").innerHTML = tabs.map((t) =>
    `<button class="tab ${FILTER === t.id ? "active" : ""}" onclick="setFilter('${esc(t.id)}')">${esc(t.name)}</button>`
  ).join("");
}

function renderStats() {
  const st = STATE.stats;
  document.getElementById("stats").innerHTML = `
    <span class="stat">已建 <b style="color:var(--green)">${st.created}</b></span>
    <span class="stat">等待 <b style="color:var(--primary)">${st.waiting}</b></span>
    <span class="stat">跳过 <b style="color:var(--red)">${st.skipped}</b></span>
    <span class="stat">未到点 ${st.upcoming}</span>
    <span class="stat">运行中 Job <b>${st.running_jobs}</b></span>`;
}

function renderLostBanner() {
  const el = document.getElementById("lost-banner");
  const n = STATE.stats.lost_receipt || 0;
  if (!n) { el.classList.add("hidden"); return; }
  el.classList.remove("hidden");
  el.innerHTML = `
    <span>⚠ 有 ${n} 个班存在“回执丢失/未确认”记录 —— 集群原 Job 仍可按幂等键查回（超期也能查）。</span>
    <button class="btn small" style="background:#fff" onclick="reconcile()">⟲ 立即全部查回</button>`;
}

function renderShifts() {
  let rows = STATE.shifts;
  if (FILTER !== "all") rows = rows.filter((s) => s.repo_id === FILTER);

  document.getElementById("shift-tbody").innerHTML = rows.map((s) => {
    const urgent = s.seconds_to_deadline >= 0 && s.seconds_to_deadline <= 60;
    const cd = s.due_ts <= STATE.sim_now_ts
      ? `<div class="countdown ${urgent ? "urgent" : ""}">
           ${s.seconds_to_deadline >= 0
             ? "窗口剩余 " + countdown(s.seconds_to_deadline)
             : "已超窗口 " + countdown(s.seconds_to_deadline)}</div>`
      : `<div class="deadline">距到点 ${countdown(s.due_ts - STATE.sim_now_ts)}</div>`;

    // Job 单元格
    let jobCell = "—";
    if (s.linked_job) {
      const j = s.linked_job;
      jobCell = `
        <div class="job-id-line"><span class="mono"><b>${esc(j.job_id)}</b></span>${jobStatusBadge(j)}</div>
        <div class="deadline">集群实际创建 ${fmtShort(j.created_ts)}</div>`;
    } else if (s.pending_lost_receipt) {
      jobCell = `<span class="id-unknown">回执已丢失 · 调度端暂无 ID</span>`;
    } else if (s.blocked_job) {
      jobCell = `<span class="deadline">被运行中 Job <span class="mono">${esc(s.blocked_job.job_id)}</span> 占用</span>`;
    }

    // 来源 / 说明
    const notes = [];
    if (s.job_source_text) notes.push(`<span class="tag">${esc(s.job_source_text)}</span>`);
    if (s.recovered_after_skip) notes.push(`<span class="tag lookup">超期跳过後查回</span>`);
    if (s.pending_lost_receipt) notes.push(`<span class="tag danger">回执丢失 · 待查回</span>`);
    if (s.status === "SKIPPED" && s.skip_reason_text)
      notes.push(`<div class="cell-note">跳过原因：${esc(s.skip_reason_text)}</div>`);
    if (s.bucket === "blocked" && s.blocked_job)
      notes.push(`<div class="cell-note">等待 <span class="mono">${esc(s.blocked_job.job_id)}</span> 结束后补建，截止 ${fmtT(s.deadline_ts)}</div>`);

    // 操作
    const ops = [];
    const due = s.due_ts <= STATE.sim_now_ts;
    if (due && s.status === "WAITING")
      ops.push(`<button class="btn small primary" onclick="runScheduling('${esc(s.repo_id)}')">触发调度</button>`);
    if (s.linked_job && s.linked_job.status === "RUNNING")
      ops.push(`<button class="btn small danger" onclick="finishJob('${esc(s.linked_job.job_id)}')">结束 Job</button>`);
    if (due && s.can_lookup)
      ops.push(`<button class="btn small ${s.pending_lost_receipt ? "warn-outline" : ""}"
                  onclick="lookupShift(${s.id})">查回集群 Job</button>`);

    const repoTag = FILTER === "all" ? `<div class="deadline">${esc(s.repo_id)}</div>` : "";
    return `
      <tr>
        <td><b>#${s.seq}</b>${repoTag}</td>
        <td>
          <div class="due">${fmtShort(s.due_ts)}</div>
          <div class="deadline">截止 ${fmtT(s.deadline_ts)}</div>
          ${cd}
        </td>
        <td>${statusBadge(s)}
            ${s.pending_lost_receipt ? `<div style="height:5px"></div><span class="badge red">回执丢失</span>` : ""}
        </td>
        <td>${jobCell}</td>
        <td>${notes.join("") || '<span class="deadline">—</span>'}</td>
        <td class="ops">${ops.join("") || "—"}</td>
      </tr>`;
  }).join("") || `<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:24px">当前筛选无班次</td></tr>`;
}

function renderClusterJobs() {
  document.getElementById("cluster-tbody").innerHTML = STATE.recent_jobs.map((j) => `
    <tr>
      <td class="mono">${esc(j.job_id)}</td>
      <td>${esc(j.repo_id)}</td>
      <td>${jobStatusBadge(j)}</td>
      <td class="mono" style="white-space:nowrap">${fmtShort(j.created_ts)}</td>
      <td class="mono" style="font-size:11px;color:var(--muted)" title="${esc(j.idempotency_key)}">${esc(j.idempotency_key)}</td>
    </tr>`).join("");
}

function renderEvents() {
  document.getElementById("event-list").innerHTML = STATE.events.map((e) => `
    <li class="${esc(e.level)}">
      <span class="t">${fmtT(e.sim_ts)}</span>
      <span class="bar"></span>
      <span class="msg">${esc(e.message)}</span>
    </li>`).join("");
}

function renderClock() {
  document.getElementById("sim-clock").textContent = fmtT(STATE.sim_now_ts);
  document.getElementById("sim-date").textContent =
    fmtDT(STATE.sim_now_ts).slice(0, 10) + "（UTC+8 模拟时间）";
}

function render() {
  if (!STATE) return;
  renderClock();
  renderRepos();
  renderTabs();
  renderStats();
  renderLostBanner();
  renderShifts();
  renderClusterJobs();
  renderEvents();
}

loadState();
setInterval(() => { if (!document.hidden) loadState(); }, 5000);
