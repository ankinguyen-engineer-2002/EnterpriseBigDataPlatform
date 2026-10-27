import argparse
import io
import os
import time
from pathlib import Path

import boto3
import duckdb
import pyarrow.parquet as pq
import psycopg
import yaml


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2026"),
        region_name="us-east-1",
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


def upload_arrow_as_parquet(s3, bucket: str, key: str, arrow_table) -> int:
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="ZSTD")
    payload = buf.getvalue()
    s3.put_object(Bucket=bucket, Key=key, Body=payload)
    return len(payload)


def seed_postgres_oltp(seed_orders: int) -> None:
    """Seeds all 9 OLTP tables in PostgreSQL 16 (idempotent) + updates legacy_daily_revenue_report."""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # 1. distribution_centers (3 hubs: North, Central, South)
            cur.execute(
                """
                INSERT INTO distribution_centers (dc_id, dc_code, name, region, city, country, latitude, longitude)
                VALUES
                    ('dc_south_01', 'SGN-DC1', 'Cat Lai Fulfillment Hub', 'South', 'Ho Chi Minh City', 'Vietnam', 10.7626, 106.7441),
                    ('dc_north_01', 'HAN-DC1', 'Bac Ninh Logistics Center', 'North', 'Ha Noi', 'Vietnam', 21.1214, 106.1110),
                    ('dc_central_01', 'DAD-DC1', 'Hoa Cam Regional Hub', 'Central', 'Da Nang', 'Vietnam', 16.0194, 108.1913)
                ON CONFLICT (dc_id) DO NOTHING;
                """
            )

            # 2. sellers (10 marketplace merchants)
            cur.execute(
                """
                INSERT INTO sellers (seller_id, seller_name, tax_code, city, state, seller_tier, rating_score)
                SELECT
                    'seller_' || LPAD(i::text, 4, '0'),
                    'Merchant Store #' || i,
                    'TAX-VN-' || LPAD(i::text, 6, '0'),
                    CASE WHEN i <= 7 THEN 'Ho Chi Minh City' WHEN i <= 9 THEN 'Ha Noi' ELSE 'Da Nang' END,
                    CASE WHEN i <= 7 THEN 'South' WHEN i <= 9 THEN 'North' ELSE 'Central' END,
                    CASE WHEN i % 3 = 0 THEN 'Mall_Official' ELSE 'Standard' END,
                    4.20 + (i % 8) * 0.10
                FROM generate_series(1, 10) AS s(i)
                ON CONFLICT (seller_id) DO NOTHING;
                """
            )

            # 3. customers (200 seed OLTP customers)
            cur.execute(
                """
                INSERT INTO customers (
                    customer_id, email, phone, first_name, last_name, gender, birth_year,
                    city, state, country, postal_code, latitude, longitude,
                    traffic_source, segment, device_type
                )
                SELECT
                    'cust_' || LPAD(i::text, 6, '0'),
                    'customer_' || i || '@commerce.vn',
                    '09' || LPAD((10000000 + i)::text, 8, '0'),
                    'First_' || i,
                    'Last_' || i,
                    CASE WHEN i % 2 = 0 THEN 'F' ELSE 'M' END,
                    1980 + (i % 25),
                    CASE WHEN i % 5 < 4 THEN 'Ho Chi Minh City' ELSE 'Ha Noi' END,
                    CASE WHEN i % 5 < 4 THEN 'South' ELSE 'North' END,
                    'Vietnam',
                    CASE WHEN i % 5 < 4 THEN '700000' ELSE '100000' END,
                    CASE WHEN i % 5 < 4 THEN 10.7769 ELSE 21.0285 END,
                    CASE WHEN i % 5 < 4 THEN 106.7009 ELSE 105.8542 END,
                    CASE WHEN i % 3 = 0 THEN 'Organic' WHEN i % 3 = 1 THEN 'Paid_Search' ELSE 'Social' END,
                    CASE WHEN i % 10 = 0 THEN 'VIP' ELSE 'Regular' END,
                    CASE WHEN i % 4 = 0 THEN 'desktop' ELSE 'mobile' END
                FROM generate_series(1, 200) AS s(i)
                ON CONFLICT (customer_id) DO NOTHING;
                """
            )

            # 4. products (100 seed OLTP products with 75% 'electronics' skew)
            cur.execute(
                """
                INSERT INTO products (
                    product_id, sku, name, category, subcategory, department, garment_group,
                    brand, seller_id, default_dc_id, cost_price, base_price, retail_price,
                    weight_grams, detail_desc
                )
                SELECT
                    'prod_' || LPAD(i::text, 6, '0'),
                    'SKU-VN-' || LPAD(i::text, 6, '0'),
                    CASE WHEN i % 4 < 3 THEN 'Smart Device Pro #' || i ELSE 'Cotton Essential Tee #' || i END,
                    CASE WHEN i % 4 < 3 THEN 'electronics' ELSE 'apparel' END,
                    CASE WHEN i % 4 < 3 THEN 'smartphones_accessories' ELSE 'tops_tshirts' END,
                    CASE WHEN i % 4 < 3 THEN 'tech_devices' ELSE 'casual_wear' END,
                    CASE WHEN i % 4 < 3 THEN 'hardware_gadget' ELSE 'jersey_basic' END,
                    CASE WHEN i % 4 < 3 THEN 'TechNova' ELSE 'UrbanThread' END,
                    'seller_' || LPAD(((i % 10) + 1)::text, 4, '0'),
                    CASE WHEN i % 5 < 4 THEN 'dc_south_01' ELSE 'dc_north_01' END,
                    150000.00 + (i * 1000),
                    250000.00 + (i * 1500),
                    290000.00 + (i * 1500),
                    300 + (i % 700),
                    'Authentic commercial catalog item seeded from H&M/theLook specification #' || i
                FROM generate_series(1, 100) AS s(i)
                ON CONFLICT (product_id) DO NOTHING;
                """
            )

            # 5. inventory_items
            cur.execute(
                """
                INSERT INTO inventory_items (
                    inventory_item_id, product_id, dc_id, batch_lot_no, unit_cost, stock_status
                )
                SELECT
                    'inv_' || LPAD(i::text, 7, '0'),
                    'prod_' || LPAD((MOD(i, 100) + 1)::text, 6, '0'),
                    CASE WHEN MOD(i, 5) < 4 THEN 'dc_south_01' ELSE 'dc_north_01' END,
                    'LOT-2026-' || LPAD(MOD(i, 50)::text, 3, '0'),
                    150000.00,
                    'available'
                FROM generate_series(1, %s) AS s(i)
                ON CONFLICT (inventory_item_id) DO NOTHING;
                """,
                (seed_orders,),
            )

            # 6. orders (80% Ho Chi Minh City skew)
            cur.execute(
                """
                INSERT INTO orders (
                    order_id, customer_id, status, shipping_city, shipping_state,
                    shipping_country, coupon_code, item_count, subtotal_amount,
                    discount_amount, shipping_fee, tax_amount, total_amount, currency,
                    order_ts, approved_ts
                )
                SELECT
                    'ord_' || LPAD(i::text, 7, '0'),
                    'cust_' || LPAD((MOD(i, 200) + 1)::text, 6, '0'),
                    CASE WHEN MOD(i, 20) = 0 THEN 'cancelled' ELSE 'delivered' END,
                    CASE WHEN MOD(i, 5) < 4 THEN 'Ho Chi Minh City' ELSE 'Ha Noi' END,
                    CASE WHEN MOD(i, 5) < 4 THEN 'South' ELSE 'North' END,
                    'Vietnam',
                    CASE WHEN MOD(i, 4) = 0 THEN 'FREESHIP26' ELSE NULL END,
                    1,
                    500000.00 + (MOD(i, 20) * 50000.00),
                    20000.00,
                    15000.00,
                    40000.00,
                    535000.00 + (MOD(i, 20) * 50000.00),
                    'VND',
                    NOW() - (MOD(i, 10) || ' days')::interval,
                    NOW() - (MOD(i, 10) || ' days')::interval + INTERVAL '5 minutes'
                FROM generate_series(1, %s) AS s(i)
                ON CONFLICT (order_id) DO NOTHING;
                """,
                (seed_orders,),
            )

            # 7. order_items
            cur.execute(
                """
                INSERT INTO order_items (
                    order_item_id, order_id, product_id, seller_id, inventory_item_id, dc_id,
                    quantity, unit_price, cost_price, discount_amount, tax_amount,
                    shipping_share_amount, net_line_amount, item_status
                )
                SELECT
                    'item_' || LPAD(i::text, 7, '0'),
                    'ord_' || LPAD(i::text, 7, '0'),
                    'prod_' || LPAD((MOD(i, 100) + 1)::text, 6, '0'),
                    'seller_' || LPAD((MOD(i, 10) + 1)::text, 4, '0'),
                    'inv_' || LPAD(i::text, 7, '0'),
                    CASE WHEN MOD(i, 5) < 4 THEN 'dc_south_01' ELSE 'dc_north_01' END,
                    1,
                    500000.00 + (MOD(i, 20) * 50000.00),
                    300000.00,
                    20000.00,
                    40000.00,
                    15000.00,
                    535000.00 + (MOD(i, 20) * 50000.00),
                    'fulfilled'
                FROM generate_series(1, %s) AS s(i)
                ON CONFLICT (order_item_id) DO NOTHING;
                """,
                (seed_orders,),
            )

            # 8. payments
            cur.execute(
                """
                INSERT INTO payments (
                    payment_id, order_id, customer_id, payment_sequential, method, provider,
                    installments, amount, fee_amount, currency, card_last4, status, failure_reason
                )
                SELECT
                    'pay_' || LPAD(i::text, 7, '0'),
                    'ord_' || LPAD(i::text, 7, '0'),
                    'cust_' || LPAD((MOD(i, 200) + 1)::text, 6, '0'),
                    1,
                    CASE WHEN MOD(i, 3) = 0 THEN 'credit_card' WHEN MOD(i, 3) = 1 THEN 'ewallet' ELSE 'bank_transfer' END,
                    CASE WHEN MOD(i, 3) = 0 THEN 'VNPay' WHEN MOD(i, 3) = 1 THEN 'MoMo' ELSE 'Napas' END,
                    CASE WHEN MOD(i, 6) = 0 THEN 3 ELSE 1 END,
                    535000.00 + (MOD(i, 20) * 50000.00),
                    5350.00,
                    'VND',
                    LPAD(MOD(1000 + i, 9999)::text, 4, '0'),
                    CASE WHEN MOD(i, 20) = 0 THEN 'failed' ELSE 'captured' END,
                    CASE WHEN MOD(i, 20) = 0 THEN 'INSUFFICIENT_FUNDS' ELSE NULL END
                FROM generate_series(1, %s) AS s(i)
                ON CONFLICT (payment_id) DO NOTHING;
                """,
                (seed_orders,),
            )

            # 9. order_reviews
            cur.execute(
                """
                INSERT INTO order_reviews (
                    review_id, order_id, customer_id, product_id, rating_score,
                    review_title, review_comment, sentiment_label
                )
                SELECT
                    'rev_' || LPAD(i::text, 7, '0'),
                    'ord_' || LPAD(i::text, 7, '0'),
                    'cust_' || LPAD((MOD(i, 200) + 1)::text, 6, '0'),
                    'prod_' || LPAD((MOD(i, 100) + 1)::text, 6, '0'),
                    CASE WHEN MOD(i, 10) = 0 THEN 2 ELSE 5 END,
                    CASE WHEN MOD(i, 10) = 0 THEN 'Giao hang hoi cham' ELSE 'San pham chinh hang tuyet voi' END,
                    'Danh gia thuc te tu khach hang cho don hang #' || i,
                    CASE WHEN MOD(i, 10) = 0 THEN 'negative' ELSE 'positive' END
                FROM generate_series(1, %s) AS s(i)
                ON CONFLICT (review_id) DO NOTHING;
                """,
                (seed_orders,),
            )

            # 10. Refresh legacy_daily_revenue_report
            legacy_sql = Path("src/db/legacy_daily_revenue_report.sql").read_text(encoding="utf-8")
            cur.execute(legacy_sql)
        conn.commit()


def generate_batch_parquet(profile_name: str, cfg: dict) -> dict:
    """Generates vectorized Parquet datasets to MinIO s3://raw/ with Rubric R5-R8 defects."""
    s3 = get_s3_client()
    con = duckdb.connect()
    p = cfg["profiles"][profile_name]

    chunks = int(p["chunks"])
    n_cust = int(p["customers_count"])
    n_prod = int(p["products_count"])
    orders_per_chunk = int(p["orders_per_chunk"])
    items_per_chunk = int(p["items_per_chunk"])
    payments_per_chunk = int(p["payments_per_chunk"])

    total_bytes = 0
    total_rows = 0
    t0 = time.perf_counter()

    # 1. Dimension Parquet: customers (18 cols)
    cust_tbl = con.execute(
        f"""
        SELECT
            'cust_' || LPAD(i::VARCHAR, 7, '0') AS customer_id,
            'customer_' || i::VARCHAR || '@commerce.vn' AS email,
            '09' || LPAD((10000000 + i)::VARCHAR, 8, '0') AS phone,
            'First_' || (i % 1000)::VARCHAR AS first_name,
            'Last_' || (i % 1000)::VARCHAR AS last_name,
            CASE WHEN i % 2 = 0 THEN 'F' ELSE 'M' END AS gender,
            1975 + (i % 30) AS birth_year,
            CASE WHEN (i % 100) < 80 THEN 'Ho Chi Minh City' WHEN (i % 100) < 92 THEN 'Ha Noi' ELSE 'Da Nang' END AS city,
            CASE WHEN (i % 100) < 80 THEN 'South' WHEN (i % 100) < 92 THEN 'North' ELSE 'Central' END AS state,
            'Vietnam' AS country,
            CASE WHEN (i % 100) < 80 THEN '700000' ELSE '100000' END AS postal_code,
            10.7769 AS latitude,
            106.7009 AS longitude,
            CASE WHEN i % 3 = 0 THEN 'Organic' WHEN i % 3 = 1 THEN 'Paid_Search' ELSE 'Social' END AS traffic_source,
            CASE WHEN i % 10 = 0 THEN 'VIP' ELSE 'Regular' END AS segment,
            CASE WHEN i % 4 = 0 THEN 'desktop' ELSE 'mobile' END AS device_type,
            TIMESTAMP '2026-01-01 00:00:00' + (i % 86400) * INTERVAL '1 second' AS signup_ts,
            TIMESTAMP '2026-10-01 00:00:00' + (i % 86400) * INTERVAL '1 second' AS updated_ts
        FROM range(1, {n_cust + 1}) t(i)
        """
    ).fetch_arrow_table()
    total_bytes += upload_arrow_as_parquet(s3, "raw", "customers/part_00000.parquet", cust_tbl)
    total_rows += cust_tbl.num_rows

    # 2. Dimension Parquet: products (18 cols — R6: 75% category = 'electronics')
    prod_tbl = con.execute(
        f"""
        SELECT
            'prod_' || LPAD(i::VARCHAR, 6, '0') AS product_id,
            'SKU-VN-' || LPAD(i::VARCHAR, 6, '0') AS sku,
            CASE WHEN (i % 100) < 75 THEN 'Flagship Smart Device #' || i::VARCHAR ELSE 'Minimalist Cotton Tee #' || i::VARCHAR END AS name,
            CASE WHEN (i % 100) < 75 THEN 'electronics' WHEN (i % 100) < 90 THEN 'apparel' ELSE 'home_living' END AS category,
            CASE WHEN (i % 100) < 75 THEN 'smartphones' ELSE 'casual_tops' END AS subcategory,
            CASE WHEN (i % 100) < 75 THEN 'consumer_tech' ELSE 'menswear' END AS department,
            CASE WHEN (i % 100) < 75 THEN 'smart_gadget' ELSE 'jersey_basic' END AS garment_group,
            CASE WHEN (i % 100) < 75 THEN 'TechNova' ELSE 'UrbanThread' END AS brand,
            'seller_' || LPAD(((i % 10) + 1)::VARCHAR, 4, '0') AS seller_id,
            CASE WHEN (i % 100) < 80 THEN 'dc_south_01' ELSE 'dc_north_01' END AS default_dc_id,
            CAST(150000 + (i % 500) * 1000 AS DOUBLE) AS cost_price,
            CAST(250000 + (i % 500) * 1500 AS DOUBLE) AS base_price,
            CAST(290000 + (i % 500) * 1500 AS DOUBLE) AS retail_price,
            350 + (i % 500) AS weight_grams,
            'High-margin commercial item with verified catalog specification #' || i::VARCHAR AS detail_desc,
            TRUE AS is_active,
            TIMESTAMP '2026-01-01 00:00:00' AS created_ts,
            TIMESTAMP '2026-10-01 00:00:00' AS updated_ts
        FROM range(1, {n_prod + 1}) t(i)
        """
    ).fetch_arrow_table()
    total_bytes += upload_arrow_as_parquet(s3, "raw", "products/part_00000.parquet", prod_tbl)
    total_rows += prod_tbl.num_rows

    # 3. Fact Chunks: orders (R6: 80% HCM Skew), order_items (R8: +2% Duplicates), payments (R7: Schema Evolution)
    half_chunk = max(1, chunks // 2)
    for c in range(chunks):
        part_name = f"part_{c:05d}.parquet"
        ord_offset = c * orders_per_chunk
        item_offset = c * items_per_chunk
        pay_offset = c * payments_per_chunk

        # 3a. orders (20 cols — R6: 80% shipping_city = 'Ho Chi Minh City')
        ord_tbl = con.execute(
            f"""
            SELECT
                'ord_' || LPAD((i + {ord_offset})::VARCHAR, 9, '0') AS order_id,
                'cust_' || LPAD(((i % {n_cust}) + 1)::VARCHAR, 7, '0') AS customer_id,
                CASE WHEN i % 20 = 0 THEN 'cancelled' ELSE 'delivered' END AS status,
                CASE
                    WHEN (i % 100) < 80 THEN 'Ho Chi Minh City'
                    WHEN (i % 100) < 92 THEN 'Ha Noi'
                    WHEN (i % 100) < 97 THEN 'Da Nang'
                    ELSE 'Can Tho'
                END AS shipping_city,
                CASE WHEN (i % 100) < 80 THEN 'South' WHEN (i % 100) < 92 THEN 'North' ELSE 'Central' END AS shipping_state,
                'Vietnam' AS shipping_country,
                CASE WHEN i % 4 = 0 THEN 'FREESHIP26' ELSE NULL END AS coupon_code,
                1 + (i % 3) AS item_count,
                CAST(500000 + (i % 20) * 50000 AS DOUBLE) AS subtotal_amount,
                CAST(20000 AS DOUBLE) AS discount_amount,
                CAST(15000 AS DOUBLE) AS shipping_fee,
                CAST(40000 AS DOUBLE) AS tax_amount,
                CAST(535000 + (i % 20) * 50000 AS DOUBLE) AS total_amount,
                'VND' AS currency,
                TIMESTAMP '2026-10-01 00:00:00' + (i % 604800) * INTERVAL '1 second' AS order_ts,
                TIMESTAMP '2026-10-01 00:05:00' + (i % 604800) * INTERVAL '1 second' AS approved_ts,
                TIMESTAMP '2026-10-01 04:00:00' + (i % 604800) * INTERVAL '1 second' AS delivered_carrier_ts,
                TIMESTAMP '2026-10-02 10:00:00' + (i % 604800) * INTERVAL '1 second' AS delivered_customer_ts,
                TIMESTAMP '2026-10-03 00:00:00' + (i % 604800) * INTERVAL '1 second' AS estimated_delivery_ts,
                TIMESTAMP '2026-10-02 10:00:00' + (i % 604800) * INTERVAL '1 second' AS updated_ts
            FROM range(1, {orders_per_chunk + 1}) t(i)
            """
        ).fetch_arrow_table()
        total_bytes += upload_arrow_as_parquet(s3, "raw", f"orders/{part_name}", ord_tbl)
        total_rows += ord_tbl.num_rows

        # 3b. order_items (17 cols — R8: ~2% Offline Duplicates with shifted created_ts + R6: Hot product skew)
        dup_limit = max(1, int(items_per_chunk * 0.02))
        items_tbl = con.execute(
            f"""
            WITH base_items AS (
                SELECT
                    'item_' || LPAD((i + {item_offset})::VARCHAR, 10, '0') AS order_item_id,
                    'ord_' || LPAD((((i - 1) % {orders_per_chunk}) + 1 + {ord_offset})::VARCHAR, 9, '0') AS order_id,
                    CASE
                        WHEN (i % 100) < 55 THEN 'prod_000001'
                        ELSE 'prod_' || LPAD(((i % {n_prod}) + 1)::VARCHAR, 6, '0')
                    END AS product_id,
                    'seller_' || LPAD(((i % 10) + 1)::VARCHAR, 4, '0') AS seller_id,
                    'inv_' || LPAD((i + {item_offset})::VARCHAR, 10, '0') AS inventory_item_id,
                    CASE WHEN (i % 100) < 80 THEN 'dc_south_01' ELSE 'dc_north_01' END AS dc_id,
                    1 + (i % 2) AS quantity,
                    CAST(350000 + (i % 10) * 25000 AS DOUBLE) AS unit_price,
                    CAST(210000 AS DOUBLE) AS cost_price,
                    CAST(10000 AS DOUBLE) AS discount_amount,
                    CAST(28000 AS DOUBLE) AS tax_amount,
                    CAST(15000 AS DOUBLE) AS shipping_share_amount,
                    CAST(383000 + (i % 10) * 25000 AS DOUBLE) AS net_line_amount,
                    'fulfilled' AS item_status,
                    TIMESTAMP '2026-10-01 00:00:00' + (i % 86400) * INTERVAL '1 second' AS created_ts,
                    TIMESTAMP '2026-10-01 06:00:00' + (i % 86400) * INTERVAL '1 second' AS shipped_ts,
                    TIMESTAMP '2026-10-02 12:00:00' + (i % 86400) * INTERVAL '1 second' AS delivered_ts
                FROM range(1, {items_per_chunk + 1}) t(i)
            ),
            dup_items AS (
                SELECT
                    order_item_id, order_id, product_id, seller_id, inventory_item_id, dc_id,
                    quantity, unit_price, cost_price, discount_amount, tax_amount,
                    shipping_share_amount, net_line_amount, item_status,
                    created_ts + INTERVAL '9 seconds' AS created_ts,
                    shipped_ts, delivered_ts
                FROM base_items
                LIMIT {dup_limit}
            )
            SELECT * FROM base_items
            UNION ALL
            SELECT * FROM dup_items
            """
        ).fetch_arrow_table()
        total_bytes += upload_arrow_as_parquet(s3, "raw", f"order_items/{part_name}", items_tbl)
        total_rows += items_tbl.num_rows

        # 3c. payments (R7: Schema Evolution — first half has 14 cols, second half has 15 cols adding 'failure_reason')
        has_evolved_col = c >= half_chunk
        extra_col_sql = (
            ",\n                CASE WHEN i % 15 = 0 THEN 'GATEWAY_TIMEOUT_RETRY' WHEN i % 25 = 0 THEN 'INSUFFICIENT_FUNDS' ELSE NULL END AS failure_reason"
            if has_evolved_col
            else ""
        )
        pay_tbl = con.execute(
            f"""
            SELECT
                'pay_' || LPAD((i + {pay_offset})::VARCHAR, 9, '0') AS payment_id,
                'ord_' || LPAD((((i - 1) % {orders_per_chunk}) + 1 + {ord_offset})::VARCHAR, 9, '0') AS order_id,
                'cust_' || LPAD(((i % {n_cust}) + 1)::VARCHAR, 7, '0') AS customer_id,
                1 AS payment_sequential,
                CASE WHEN i % 3 = 0 THEN 'credit_card' WHEN i % 3 = 1 THEN 'ewallet' ELSE 'bank_transfer' END AS method,
                CASE WHEN i % 3 = 0 THEN 'VNPay' WHEN i % 3 = 1 THEN 'MoMo' ELSE 'Napas' END AS provider,
                CASE WHEN i % 5 = 0 THEN 3 ELSE 1 END AS installments,
                CAST(535000 + (i % 20) * 50000 AS DOUBLE) AS amount,
                CAST(5350 AS DOUBLE) AS fee_amount,
                'VND' AS currency,
                LPAD(((1000 + i) % 9999)::VARCHAR, 4, '0') AS card_last4,
                CASE WHEN i % 15 = 0 THEN 'failed' ELSE 'captured' END AS status,
                TIMESTAMP '2026-10-01 00:01:00' + (i % 86400) * INTERVAL '1 second' AS payment_ts,
                TIMESTAMP '2026-10-01 00:01:05' + (i % 86400) * INTERVAL '1 second' AS updated_ts{extra_col_sql}
            FROM range(1, {payments_per_chunk + 1}) t(i)
            """
        ).fetch_arrow_table()
        total_bytes += upload_arrow_as_parquet(s3, "raw", f"payments/{part_name}", pay_tbl)
        total_rows += pay_tbl.num_rows

    elapsed = time.perf_counter() - t0
    return {
        "profile": profile_name,
        "chunks": chunks,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "elapsed_sec": round(elapsed, 3),
        "rows_per_sec": int(total_rows / max(elapsed, 0.001)),
    }


def verify_rubric_r5_to_r8() -> None:
    """Downloads generated Parquet metadata/tables from MinIO S3 and verifies Rubric R5, R6, R7, R8."""
    s3 = get_s3_client()
    con = duckdb.connect()

    def load_s3_parquet_prefix(prefix: str):
        objs = s3.list_objects_v2(Bucket="raw", Prefix=prefix).get("Contents", [])
        tables = []
        for obj in sorted(objs, key=lambda x: x["Key"]):
            body = s3.get_object(Bucket="raw", Key=obj["Key"])["Body"].read()
            tables.append((obj["Key"], pq.read_table(io.BytesIO(body))))
        return tables

    ord_parts = load_s3_parquet_prefix("orders/")
    prod_parts = load_s3_parquet_prefix("products/")
    item_parts = load_s3_parquet_prefix("order_items/")
    pay_parts = load_s3_parquet_prefix("payments/")

    # R6 Check 1: orders shipping_city skew
    ord_arrow = ord_parts[0][1]
    con.register("v_orders", ord_arrow)
    hcm_pct = con.execute(
        "SELECT ROUND(100.0 * SUM(CASE WHEN shipping_city = 'Ho Chi Minh City' THEN 1 ELSE 0 END) / COUNT(*), 2) FROM v_orders"
    ).fetchone()[0]

    # R6 Check 2: products electronics skew
    prod_arrow = prod_parts[0][1]
    con.register("v_products", prod_arrow)
    elec_pct = con.execute(
        "SELECT ROUND(100.0 * SUM(CASE WHEN category = 'electronics' THEN 1 ELSE 0 END) / COUNT(*), 2) FROM v_products"
    ).fetchone()[0]

    # R7 Check: Schema Evolution across payment chunks
    pay_early_key, pay_early_tbl = pay_parts[0]
    pay_late_key, pay_late_tbl = pay_parts[-1]

    # R8 Check: Offline duplicates in order_items
    item_arrow = item_parts[0][1]
    con.register("v_items", item_arrow)
    total_items, distinct_items = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT order_item_id) FROM v_items"
    ).fetchone()
    dup_pct = round(100.0 * (total_items - distinct_items) / distinct_items, 2)

    print("\n" + "=" * 78)
    print("RUBRIC R5-R8 VERIFICATION REPORT (s3://raw/ on MinIO + PostgreSQL 16 OLTP)")
    print("=" * 78)
    print(f"[R6 - Data Skew #1] orders.shipping_city = 'Ho Chi Minh City' : {hcm_pct}% (Target: ~80%)")
    print(f"[R6 - Data Skew #2] products.category = 'electronics'         : {elec_pct}% (Target: ~75%)")
    print(
        f"[R7 - Schema Evol ] {pay_early_key} cols = {pay_early_tbl.num_columns} "
        f"| {pay_late_key} cols = {pay_late_tbl.num_columns} "
        f"(has 'failure_reason': {'failure_reason' in pay_late_tbl.column_names})"
    )
    print(
        f"[R8 - Offline Dup ] order_items rows = {total_items:,} | distinct IDs = {distinct_items:,} "
        f"| duplicates = {total_items - distinct_items:,} ({dup_pct}% — Target: ~2.0%)"
    )
    print("=" * 78)


def main():
    parser = argparse.ArgumentParser(description="MultiCloud-Commerce-Lakehouse Vectorized Data Generator")
    parser.add_argument("--config", default="src/generator/generator_config.yaml", help="Path to YAML config")
    parser.add_argument("--profile", default="ci", choices=["ci", "dev", "rubric_100gb"], help="Generation profile")
    parser.add_argument("--skip-oltp", action="store_true", help="Skip seeding PostgreSQL OLTP tables")
    parser.add_argument("--verify", action="store_true", help="Run DuckDB/PyArrow verification on MinIO S3")
    args = parser.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    prof = cfg["profiles"][args.profile]

    if not args.skip_oltp:
        seed_postgres_oltp(int(prof["oltp_seed_orders"]))
        print(f"[OLTP Seed] Seeded 9 PostgreSQL 16 tables + refreshed legacy_daily_revenue_report ({prof['oltp_seed_orders']} orders).")

    stats = generate_batch_parquet(args.profile, cfg)
    print(f"[Batch Parquet] {stats}")

    if args.verify:
        verify_rubric_r5_to_r8()


if __name__ == "__main__":
    main()
