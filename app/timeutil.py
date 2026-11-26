from __future__ import annotations

from datetime import datetime

from .config import DISPLAY_TZ


def fmt_ts(ts: int | None, with_seconds: bool = True) -> str:
    if ts is None:
        return "—"
    dt = datetime.fromtimestamp(int(ts), tz=DISPLAY_TZ)
    fmt = "%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M"
    return dt.strftime(fmt)


def fmt_clock(ts: int | None) -> str:
    """顶栏大时钟，只显示日期 + 时分秒。"""
    if ts is None:
        return "—"
    return datetime.fromtimestamp(int(ts), tz=DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S")


def fmt_hm(ts: int | None) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(int(ts), tz=DISPLAY_TZ).strftime("%H:%M:%S")


def countdown(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"
