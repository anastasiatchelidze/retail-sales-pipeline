-- ============================================================
-- Retail Sales Pipeline
-- Transformation Layer
--
-- The pipeline uses a full-refresh strategy:
-- staging is replaced and all derived tables are rebuilt.
-- ============================================================


-- ============================================================
-- Reset derived tables
-- ============================================================

TRUNCATE TABLE
    analytics.daily_store_product_sales,
    warehouse.dim_lfl_store,
    warehouse.fact_sales,
    warehouse.dim_product,
    warehouse.dim_store
RESTART IDENTITY;


-- ============================================================
-- Store dimension
--
-- StoreLocation is the source-level store identifier.
-- Generated store IDs are deterministic warehouse identifiers.
-- ============================================================

WITH stores AS (
    SELECT DISTINCT
        TRIM(store_location) AS source_location
    FROM staging.sales_raw
    WHERE store_location IS NOT NULL
      AND TRIM(store_location) <> ''
),
numbered_stores AS (
    SELECT
        source_location,
        ROW_NUMBER() OVER (
            ORDER BY source_location
        ) AS store_number
    FROM stores
)
INSERT INTO warehouse.dim_store (
    store_id,
    store_name,
    source_location
)
SELECT
    'STORE_' ||
        LPAD(
            store_number::TEXT,
            GREATEST(
                6,
                LENGTH(store_number::TEXT)
            ),
            '0'
        ),
    'Store ' ||
        LPAD(
            store_number::TEXT,
            GREATEST(
                6,
                LENGTH(store_number::TEXT)
            ),
            '0'
        ),
    source_location
FROM numbered_stores;


-- ============================================================
-- Product dimension
--
-- ProductCategory remains outside the product dimension because
-- the source data contains multiple categories for the same
-- product_id.
-- ============================================================

WITH products AS (
    SELECT DISTINCT
        TRIM(product_id) AS product_id
    FROM staging.sales_raw
    WHERE product_id IS NOT NULL
      AND TRIM(product_id) <> ''
),
numbered_products AS (
    SELECT
        product_id,
        ROW_NUMBER() OVER (
            ORDER BY product_id
        ) AS product_number
    FROM products
)
INSERT INTO warehouse.dim_product (
    product_id,
    product_name
)
SELECT
    product_id,
    'Product ' ||
        LPAD(
            product_number::TEXT,
            GREATEST(
                6,
                LENGTH(product_number::TEXT)
            ),
            '0'
        )
FROM numbered_products;


-- ============================================================
-- Sales fact
--
-- Product category remains at transaction level because it is
-- not functionally dependent on product_id in the source data.
-- ============================================================

INSERT INTO warehouse.fact_sales (
    customer_id,
    product_id,
    store_id,
    transaction_date,
    quantity,
    price,
    product_category,
    discount_applied,
    total_amount,
    payment_method
)
SELECT
    NULLIF(TRIM(raw.customer_id), ''),
    raw.product_id,
    store.store_id,
    raw.transaction_date,
    raw.quantity,
    raw.price,
    NULLIF(TRIM(raw.product_category), ''),
    raw.discount_applied,
    raw.total_amount,
    NULLIF(TRIM(raw.payment_method), '')
FROM staging.sales_raw raw
JOIN warehouse.dim_store store
    ON store.source_location = TRIM(raw.store_location)
JOIN warehouse.dim_product product
    ON product.product_id = raw.product_id;


-- ============================================================
-- Daily store-product aggregation
--
-- Grain:
--   one row per sales date, store, and product.
--
-- Discount is weighted by realized transaction revenue.
-- Null discounts are excluded from the weighted calculation.
-- ============================================================

INSERT INTO analytics.daily_store_product_sales (
    sales_date,
    store_id,
    product_id,
    total_quantity,
    total_revenue,
    total_transaction_count,
    unique_customers,
    weighted_discount_pct
)
SELECT
    transaction_date::DATE AS sales_date,
    store_id,
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue,
    COUNT(*) AS total_transaction_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    CASE
        WHEN SUM(total_amount) = 0 THEN NULL
        ELSE
            SUM(
                discount_applied * total_amount
            ) / SUM(
                CASE
                    WHEN discount_applied IS NOT NULL
                    THEN total_amount
                    ELSE 0
                END
            )
    END AS weighted_discount_pct
FROM warehouse.fact_sales
GROUP BY
    transaction_date::DATE,
    store_id,
    product_id;


-- ============================================================
-- Like-for-like store analysis
--
-- Current year = latest year in the dataset.
--
-- A store-month is LFL when:
--   1. The store traded during the same month in the
--      previous year.
--   2. The current-year store has sales on every expected
--      day of that month.
--
-- The latest month uses the latest transaction date as the
-- month boundary, allowing partially available months.
--
-- Date arithmetic handles leap years automatically.
-- ============================================================

WITH date_bounds AS (
    SELECT
        MAX(
            EXTRACT(YEAR FROM transaction_date)::INTEGER
        ) AS current_year,
        MAX(transaction_date)::DATE AS latest_date
    FROM warehouse.fact_sales
),
current_store_months AS (
    SELECT
        f.store_id,
        EXTRACT(
            YEAR FROM f.transaction_date
        )::INTEGER AS year,
        EXTRACT(
            MONTH FROM f.transaction_date
        )::INTEGER AS month,
        COUNT(
            DISTINCT f.transaction_date::DATE
        ) AS sales_days
    FROM warehouse.fact_sales f
    CROSS JOIN date_bounds d
    WHERE EXTRACT(
        YEAR FROM f.transaction_date
    )::INTEGER = d.current_year
    GROUP BY
        f.store_id,
        EXTRACT(
            YEAR FROM f.transaction_date
        )::INTEGER,
        EXTRACT(
            MONTH FROM f.transaction_date
        )::INTEGER
),
previous_store_months AS (
    SELECT DISTINCT
        f.store_id,
        EXTRACT(
            MONTH FROM f.transaction_date
        )::INTEGER AS month
    FROM warehouse.fact_sales f
    CROSS JOIN date_bounds d
    WHERE EXTRACT(
        YEAR FROM f.transaction_date
    )::INTEGER = d.current_year - 1
),
lfl_comparison AS (
    SELECT
        current_month.store_id,
        current_month.year,
        current_month.month,
        current_month.sales_days,
        CASE
            WHEN MAKE_DATE(
                current_month.year,
                current_month.month,
                1
            ) = DATE_TRUNC(
                'month',
                bounds.latest_date
            )::DATE
            THEN EXTRACT(
                DAY FROM bounds.latest_date
            )::INTEGER

            ELSE EXTRACT(
                DAY FROM (
                    MAKE_DATE(
                        current_month.year,
                        current_month.month,
                        1
                    )
                    + INTERVAL '1 month'
                    - INTERVAL '1 day'
                )
            )::INTEGER
        END AS expected_days,
        previous_month.store_id IS NOT NULL
            AS traded_previous_year
    FROM current_store_months current_month
    CROSS JOIN date_bounds bounds
    LEFT JOIN previous_store_months previous_month
        ON previous_month.store_id = current_month.store_id
       AND previous_month.month = current_month.month
)
INSERT INTO warehouse.dim_lfl_store (
    year,
    month,
    store_id,
    lfl_status
)
SELECT
    year,
    month,
    store_id,
    traded_previous_year
        AND sales_days = expected_days
FROM lfl_comparison;


-- ============================================================
-- ABC analysis
--
-- ABC is calculated separately by:
--   1. Quantity
--   2. Revenue
--
-- Classification:
--   A = cumulative share <= 50%
--   B = cumulative share <= 70%
--   C = cumulative share > 70%
--
-- Product ID is used as a deterministic tie-breaker.
-- ============================================================

WITH product_realization AS (
    SELECT
        product_id,
        SUM(total_quantity) AS realization_quantity,
        SUM(total_revenue) AS realization_amount
    FROM analytics.daily_store_product_sales
    GROUP BY product_id
),
quantity_analysis AS (
    SELECT
        product_id,
        realization_quantity,
        SUM(realization_quantity) OVER (
            ORDER BY
                realization_quantity DESC,
                product_id
            ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        )
        / NULLIF(
            SUM(realization_quantity) OVER (),
            0
        ) AS cumulative_quantity_share
    FROM product_realization
),
amount_analysis AS (
    SELECT
        product_id,
        realization_amount,
        SUM(realization_amount) OVER (
            ORDER BY
                realization_amount DESC,
                product_id
            ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        )
        / NULLIF(
            SUM(realization_amount) OVER (),
            0
        ) AS cumulative_amount_share
    FROM product_realization
),
abc_classification AS (
    SELECT
        quantity_analysis.product_id,

        CASE
            WHEN quantity_analysis.cumulative_quantity_share <= 0.50
                THEN 'A'
            WHEN quantity_analysis.cumulative_quantity_share <= 0.70
                THEN 'B'
            ELSE 'C'
        END AS abc_quantity,

        CASE
            WHEN amount_analysis.cumulative_amount_share <= 0.50
                THEN 'A'
            WHEN amount_analysis.cumulative_amount_share <= 0.70
                THEN 'B'
            ELSE 'C'
        END AS abc_amount

    FROM quantity_analysis
    JOIN amount_analysis
        ON amount_analysis.product_id =
           quantity_analysis.product_id
)
UPDATE warehouse.dim_product product
SET
    abc_quantity = classification.abc_quantity,
    abc_amount = classification.abc_amount
FROM abc_classification classification
WHERE product.product_id = classification.product_id;