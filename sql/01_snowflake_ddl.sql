-- ============================================================
-- SNITCH Marketplace Intelligence - Snowflake DDL
-- Run this in Snowflake worksheet after creating a free trial account
-- ============================================================

-- 1. Setup database and schema
CREATE DATABASE IF NOT EXISTS SNITCH_ANALYTICS;
USE DATABASE SNITCH_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS MARTS;
USE SCHEMA RAW;

-- 2. Dimension tables
CREATE OR REPLACE TABLE DIM_SKU (
    sku_id          VARCHAR(10) PRIMARY KEY,
    category        VARCHAR(50),
    mrp             NUMBER(10,2),
    cogs            NUMBER(10,2),
    packaging_cost  NUMBER(10,2)
);

CREATE OR REPLACE TABLE DIM_STORE (
    store_id        VARCHAR(30) PRIMARY KEY,
    city            VARCHAR(50),
    store_type      VARCHAR(20),
    sqft            NUMBER(8,0),
    opened_date     DATE
);

-- 3. Fact tables
CREATE OR REPLACE TABLE FACT_ORDERS (
    order_id        VARCHAR(20) PRIMARY KEY,
    order_date      DATE,
    city            VARCHAR(50),
    channel         VARCHAR(20),
    store_id        VARCHAR(30),
    sku_id          VARCHAR(10),
    category        VARCHAR(50),
    mrp             NUMBER(10,2),
    selling_price   NUMBER(10,2),
    discount_pct    NUMBER(6,4),
    qty             NUMBER(4,0),
    gross_revenue   NUMBER(12,2),
    commission      NUMBER(12,2),
    logistics_cost  NUMBER(10,2),
    cogs            NUMBER(12,2),
    packaging_cost  NUMBER(10,2)
);

CREATE OR REPLACE TABLE FACT_RETURNS (
    return_id              VARCHAR(20) PRIMARY KEY,
    order_id               VARCHAR(20),
    return_date            DATE,
    return_reason          VARCHAR(100),
    refund_amount          NUMBER(12,2),
    reverse_logistics_cost NUMBER(10,2)
);

CREATE OR REPLACE TABLE FACT_MARKETING_SPEND (
    spend_date       DATE,
    channel          VARCHAR(20),
    category         VARCHAR(50),
    marketing_spend  NUMBER(12,2)
);

-- 4. File format + stage for CSV loading
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL');

CREATE OR REPLACE STAGE snitch_stage
    FILE_FORMAT = csv_format;

-- 5. Upload CSVs from local using SnowSQL or the Snowsight UI:
--    PUT file:///path/to/data/dim_sku.csv @snitch_stage;
--    PUT file:///path/to/data/dim_store.csv @snitch_stage;
--    PUT file:///path/to/data/fact_orders.csv @snitch_stage;
--    PUT file:///path/to/data/fact_returns.csv @snitch_stage;
--    PUT file:///path/to/data/fact_marketing_spend.csv @snitch_stage;
--
-- Or use Snowsight: Data > Add Data > Load files into existing tables.

-- 6. COPY commands
COPY INTO DIM_SKU              FROM @snitch_stage/dim_sku.csv;
COPY INTO DIM_STORE            FROM @snitch_stage/dim_store.csv;
COPY INTO FACT_ORDERS          FROM @snitch_stage/fact_orders.csv;
COPY INTO FACT_RETURNS         FROM @snitch_stage/fact_returns.csv;
COPY INTO FACT_MARKETING_SPEND FROM @snitch_stage/fact_marketing_spend.csv;

-- 7. Sanity checks
SELECT COUNT(*) AS orders FROM FACT_ORDERS;
SELECT COUNT(*) AS returns FROM FACT_RETURNS;
SELECT MIN(order_date), MAX(order_date) FROM FACT_ORDERS;
