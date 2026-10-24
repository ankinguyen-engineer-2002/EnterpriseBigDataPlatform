# ==========================================
# STAGE 1: Builder (Xưởng biên dịch thư viện)
# ==========================================
FROM python:3.11-slim-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt
# ==========================================
# STAGE 2: Runtime (Môi trường chạy thực tế siêu nhẹ)
# ==========================================
FROM python:3.11-slim-bookworm AS runtime
WORKDIR /app
# Tạo tài khoản non-root 'edpuser' (UID 10001) để chống leo thang đặc quyền
RUN groupadd -g 10001 edpgrp && \
    useradd -u 10001 -g edpgrp -m -s /sbin/nologin edpuser
# Chỉ copy thư viện đã cài đặt từ Stage 1 sang (bỏ lại gcc, apt cache ở Stage 1)
COPY --from=builder /install /usr/local
COPY --chown=edpuser:edpgrp src/ ./src/
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app
USER edpuser
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')" || exit 1
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
