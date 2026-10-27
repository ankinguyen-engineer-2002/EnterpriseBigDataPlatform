import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="MultiCloud-Commerce-Lakehouse - Ingestion & Order API",
    version="1.1.0",
)


def get_pg_conn():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "edp_postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "commerce_oltp"),
        user=os.getenv("POSTGRES_USER", "edp_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "edp_secret_2026"),
        autocommit=False,
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2026"),
        region_name="us-east-1",
    )


class OrderCreateContract(BaseModel):
    order_id: str = Field(..., min_length=3)
    customer_id: str = Field(..., min_length=3)
    shipping_city: str = Field(default="Ho Chi Minh City")
    shipping_state: str = Field(default="South")
    subtotal_amount: float = Field(..., gt=0)
    discount_amount: float = Field(default=0.0, ge=0)
    shipping_fee: float = Field(default=15000.0, ge=0)
    tax_amount: float = Field(default=0.0, ge=0)
    total_amount: float = Field(..., gt=0)
    currency: str = Field(default="VND", min_length=3, max_length=3)
    coupon_code: Optional[str] = None


@app.exception_handler(RequestValidationError)
async def validation_exception_to_dlq_handler(request: Request, exc: RequestValidationError):
    """Tự động bắt mọi đơn hàng bẩn vi phạm Pydantic Contract và cô lập vào MinIO s3://raw/dlq/."""
    raw_body = (await request.body()).decode("utf-8", errors="replace")
    dlq_id = f"dlq_{uuid.uuid4().hex[:12]}"
    now_iso = datetime.now(timezone.utc).isoformat()

    dlq_envelope = {
        "dlq_id": dlq_id,
        "source_topic": str(request.url.path),
        "raw_payload": raw_body,
        "error_code": "SCHEMA_CONTRACT_VIOLATION",
        "error_message": json.dumps(exc.errors(), default=str),
        "schema_version": "v1.1.0",
        "trace_id": uuid.uuid4().hex,
        "failed_ts": now_iso,
        "replay_status": "QUARANTINED",
    }

    s3_key = f"dlq/dt={now_iso[:10]}/{dlq_id}.json"
    dlq_s3_uri = f"s3://raw/{s3_key}"
    try:
        s3 = get_s3_client()
        s3.put_object(
            Bucket="raw",
            Key=s3_key,
            Body=json.dumps(dlq_envelope, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as s3_err:
        dlq_envelope["dlq_storage_warning"] = str(s3_err)

    return JSONResponse(
        status_code=422,
        content={
            "status": "quarantined_to_dlq",
            "dlq_s3_uri": dlq_s3_uri,
            "dlq_envelope": dlq_envelope,
        },
    )


@app.get("/health")
def health_check() -> dict:
    pg_ok = False
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                pg_ok = cur.fetchone()[0] == 1
    except Exception:
        pg_ok = False

    return {
        "status": "healthy" if pg_ok else "degraded",
        "service": "edp-order-ingestion-api",
        "postgres_connected": pg_ok,
        "postgres_db": os.getenv("POSTGRES_DB", "commerce_oltp"),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/v1/orders/validate")
def validate_order_contract(order: OrderCreateContract) -> dict:
    return {
        "valid": True,
        "order_id": order.order_id,
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/v1/orders")
def ingest_order(order: OrderCreateContract) -> dict:
    """Ghi đơn hàng hợp lệ vào PostgreSQL 16 (sinh WAL CDC) và đồng bộ bảng đối soát Legacy."""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1. Đảm bảo khách hàng tồn tại để thỏa mãn khóa ngoại FOREIGN KEY
            cur.execute(
                """
                INSERT INTO customers (
                    customer_id, email, phone, first_name, last_name,
                    gender, birth_year, city, state, country,
                    postal_code, latitude, longitude, traffic_source, segment, device_type
                ) VALUES (
                    %s, %s, '0900000000', 'Live', 'Customer',
                    'U', 1995, %s, %s, 'Vietnam',
                    '700000', 10.7769, 106.7009, 'API', 'Regular', 'mobile'
                ) ON CONFLICT (customer_id) DO NOTHING;
                """,
                (
                    order.customer_id,
                    f"{order.customer_id}@example.com",
                    order.shipping_city,
                    order.shipping_state,
                ),
            )

            # 2. Ghi đơn hàng vào bảng orders (Idempotent Upsert + kích hoạt WAL CDC)
            cur.execute(
                """
                INSERT INTO orders (
                    order_id, customer_id, status, shipping_city, shipping_state,
                    coupon_code, subtotal_amount, discount_amount, shipping_fee,
                    tax_amount, total_amount, currency
                ) VALUES (
                    %s, %s, 'approved', %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    total_amount = EXCLUDED.total_amount,
                    updated_ts = NOW();
                """,
                (
                    order.order_id,
                    order.customer_id,
                    order.shipping_city,
                    order.shipping_state,
                    order.coupon_code,
                    order.subtotal_amount,
                    order.discount_amount,
                    order.shipping_fee,
                    order.tax_amount,
                    order.total_amount,
                    order.currency,
                ),
            )
        conn.commit()

    return {
        "status": "ingested",
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "total_amount": order.total_amount,
        "cdc_wal_emitted": True,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
