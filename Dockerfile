FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data \
    HOME=/home/drill

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --uid 1000 --create-home drill \
    && mkdir -p /data \
    && chown -R drill:drill /data
# /app 代码保持 root:root 只读即可，进程用非特权用户运行
USER drill

VOLUME ["/data"]
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
    CMD python -c "import json,urllib.request,sys; r=urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=3); sys.exit(0 if r.status==200 and json.load(r)['ok'] else 1)"

# 多 worker：并发触发调度时，由两个 SQLite 库的事务/唯一约束做最终裁决
# --preload：在 master 进程先建好表，避免多个 worker 同时初始化 schema
CMD ["gunicorn", "--preload", "--workers", "4", "--worker-class", "sync", "--bind", "0.0.0.0:8000", "--timeout", "30", "--access-logfile", "-", "--error-logfile", "-", "app.web:app"]
