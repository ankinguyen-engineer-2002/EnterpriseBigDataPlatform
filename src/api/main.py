import os
from datetime import datetime, timezone
from fastapi import FastAPI
from pydantic import BaseModel, Field

app = FastAPI(
    title="MultiCloud-Commerce-Lakehouse - Ingestion & Order API",
    version="1.0.0",
)


class OrderCreateContract(BaseModel):
    order_id: str = Field(..., min_length=3)
    customer_id: str = Field(..., min_length=3)
    shipping_city: str = Field(default="Ho Chi Minh City")
    total_amount: float = Field(..., gt=0)
    currency: str = Field(default="VND", min_length=3, max_length=3)


@app.get("/health")
def health_check() -> dict:
    return {
        "status": "healthy",
        "service": "edp-order-ingestion-api",
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
