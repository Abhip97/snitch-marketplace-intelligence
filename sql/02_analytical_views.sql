-- ============================================================
-- SNITCH Marketplace Intelligence - Analytical Views
-- These views power the Power BI dashboard and AI workflows
-- ============================================================
USE DATABASE SNITCH_ANALYTICS;
USE SCHEMA MARTS;

-- ------------------------------------------------------------
-- VIEW 1: Daily P&L by Channel x City x Category
-- The single most important view. Owns the "daily P&L visibility"
-- requirement from the JD.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_DAILY_PNL AS
WITH order_pnl AS (
    SELECT
        o.order_date,
        o.city,
        o.channel,
        o.category,
        COUNT(DISTINCT o.order_id)              AS order_count,
        SUM(o.qty)                              AS units_sold,
        SUM(o.mrp * o.qty)                      AS gross_mrp,
        SUM(o.gross_revenue)                    AS gross_revenue,
        SUM(o.gross_revenue) - SUM(o.mrp * o.qty) AS discount_value,
        SUM(o.commission)                       AS marketplace_commission,
        SUM(o.cogs)                             AS cogs,
        SUM(o.packaging_cost)                   AS packaging,
        SUM(o.logistics_cost)                   AS logistics
    FROM RAW.FACT_ORDERS o
    GROUP BY 1,2,3,4
),
return_pnl AS (
    SELECT
        o.order_date,
        o.city,
        o.channel,
        o.category,
        SUM(r.refund_amount)              AS refund_value,
        SUM(r.reverse_logistics_cost)     AS reverse_logistics,
        COUNT(*)                          AS returns_count
    FROM RAW.FACT_RETURNS r
    JOIN RAW.FACT_ORDERS o ON r.order_id = o.order_id
    GROUP BY 1,2,3,4
)
SELECT
    op.order_date,
    op.city,
    op.channel,
    op.category,
    op.order_count,
    op.units_sold,
    COALESCE(rp.returns_count, 0)             AS returns_count,
    op.gross_mrp,
    op.gross_revenue,
    op.discount_value,
    COALESCE(rp.refund_value, 0)              AS refund_value,
    op.gross_revenue - COALESCE(rp.refund_value, 0) AS net_revenue,
    op.marketplace_commission,
    op.cogs,
    op.packaging,
    op.logistics,
    COALESCE(rp.reverse_logistics, 0)         AS reverse_logistics,
    -- Contribution margin = net revenue - commission - COGS - packaging - logistics - reverse logistics
    op.gross_revenue
        - COALESCE(rp.refund_value, 0)
        - op.marketplace_commission
        - op.cogs
        - op.packaging
        - op.logistics
        - COALESCE(rp.reverse_logistics, 0)   AS contribution_margin,
    -- Margin %
    DIV0(
        op.gross_revenue - COALESCE(rp.refund_value, 0) - op.marketplace_commission
            - op.cogs - op.packaging - op.logistics - COALESCE(rp.reverse_logistics, 0),
        op.gross_revenue - COALESCE(rp.refund_value, 0)
    ) AS contribution_margin_pct
FROM order_pnl op
LEFT JOIN return_pnl rp
    ON op.order_date = rp.order_date
    AND op.city = rp.city
    AND op.channel = rp.channel
    AND op.category = rp.category;

-- ------------------------------------------------------------
-- VIEW 2: Returns Deep Dive
-- "Returns analytics" - reason mix, return rate, true margin impact
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_RETURNS_ANALYTICS AS
SELECT
    o.category,
    o.channel,
    o.city,
    r.return_reason,
    COUNT(*)                                    AS return_count,
    SUM(r.refund_amount)                        AS total_refund,
    SUM(r.reverse_logistics_cost)               AS total_reverse_logistics,
    AVG(DATEDIFF('day', o.order_date, r.return_date)) AS avg_days_to_return
FROM RAW.FACT_RETURNS r
JOIN RAW.FACT_ORDERS o ON r.order_id = o.order_id
GROUP BY 1,2,3,4;

-- Return rate by category x channel (gives the % view)
CREATE OR REPLACE VIEW V_RETURN_RATE AS
SELECT
    o.category,
    o.channel,
    COUNT(DISTINCT o.order_id)            AS total_orders,
    COUNT(DISTINCT r.return_id)           AS returned_orders,
    DIV0(COUNT(DISTINCT r.return_id), COUNT(DISTINCT o.order_id)) AS return_rate
FROM RAW.FACT_ORDERS o
LEFT JOIN RAW.FACT_RETURNS r ON o.order_id = r.order_id
GROUP BY 1,2;

-- ------------------------------------------------------------
-- VIEW 3: Channel Price Parity Check
-- Pricing model #1 - is the same SKU underpriced on one marketplace?
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_PRICE_PARITY AS
SELECT
    sku_id,
    category,
    mrp,
    AVG(CASE WHEN channel = 'D2C_APP'  THEN selling_price END) AS asp_d2c_app,
    AVG(CASE WHEN channel = 'D2C_WEB'  THEN selling_price END) AS asp_d2c_web,
    AVG(CASE WHEN channel = 'MYNTRA'   THEN selling_price END) AS asp_myntra,
    AVG(CASE WHEN channel = 'FLIPKART' THEN selling_price END) AS asp_flipkart,
    AVG(CASE WHEN channel = 'AJIO'     THEN selling_price END) AS asp_ajio,
    AVG(CASE WHEN channel = 'OFFLINE'  THEN selling_price END) AS asp_offline,
    -- Spread = max - min ASP across channels (high spread = parity issue)
    GREATEST(
        COALESCE(AVG(CASE WHEN channel = 'D2C_APP'  THEN selling_price END), 0),
        COALESCE(AVG(CASE WHEN channel = 'MYNTRA'   THEN selling_price END), 0),
        COALESCE(AVG(CASE WHEN channel = 'FLIPKART' THEN selling_price END), 0),
        COALESCE(AVG(CASE WHEN channel = 'AJIO'     THEN selling_price END), 0)
    ) -
    LEAST(
        NULLIF(COALESCE(AVG(CASE WHEN channel = 'D2C_APP'  THEN selling_price END), 99999), 0),
        NULLIF(COALESCE(AVG(CASE WHEN channel = 'MYNTRA'   THEN selling_price END), 99999), 0),
        NULLIF(COALESCE(AVG(CASE WHEN channel = 'FLIPKART' THEN selling_price END), 99999), 0),
        NULLIF(COALESCE(AVG(CASE WHEN channel = 'AJIO'     THEN selling_price END), 99999), 0)
    ) AS price_spread
FROM RAW.FACT_ORDERS
GROUP BY 1,2,3;

-- ------------------------------------------------------------
-- VIEW 4: SKU Profitability (post-return)
-- The "silently bleeding SKUs" view - true margin after returns
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_SKU_PROFITABILITY AS
WITH sku_orders AS (
    SELECT
        sku_id,
        category,
        SUM(qty)            AS units_sold,
        SUM(gross_revenue)  AS gross_revenue,
        SUM(commission)     AS commission,
        SUM(cogs)           AS cogs,
        SUM(packaging_cost) AS packaging,
        SUM(logistics_cost) AS logistics
    FROM RAW.FACT_ORDERS
    GROUP BY 1,2
),
sku_returns AS (
    SELECT
        o.sku_id,
        SUM(r.refund_amount)          AS refunds,
        SUM(r.reverse_logistics_cost) AS rev_log,
        COUNT(*)                      AS return_count
    FROM RAW.FACT_RETURNS r
    JOIN RAW.FACT_ORDERS o ON r.order_id = o.order_id
    GROUP BY 1
)
SELECT
    so.sku_id,
    so.category,
    so.units_sold,
    so.gross_revenue,
    COALESCE(sr.return_count, 0)    AS returns,
    DIV0(COALESCE(sr.return_count, 0), so.units_sold) AS return_rate,
    so.gross_revenue - so.commission - so.cogs - so.packaging - so.logistics AS gross_margin,
    so.gross_revenue
        - COALESCE(sr.refunds, 0)
        - so.commission - so.cogs - so.packaging - so.logistics
        - COALESCE(sr.rev_log, 0) AS true_margin_post_returns,
    DIV0(
        so.gross_revenue
            - COALESCE(sr.refunds, 0)
            - so.commission - so.cogs - so.packaging - so.logistics
            - COALESCE(sr.rev_log, 0),
        so.gross_revenue - COALESCE(sr.refunds, 0)
    ) AS true_margin_pct
FROM sku_orders so
LEFT JOIN sku_returns sr ON so.sku_id = sr.sku_id;

-- ------------------------------------------------------------
-- VIEW 5: Marketing Efficiency by Category
-- Contribution margin vs marketing spend
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_MARKETING_EFFICIENCY AS
WITH cat_pnl AS (
    SELECT
        category,
        channel,
        SUM(contribution_margin) AS contribution_margin,
        SUM(net_revenue)         AS net_revenue
    FROM V_DAILY_PNL
    GROUP BY 1,2
),
cat_spend AS (
    SELECT
        category,
        channel,
        SUM(marketing_spend) AS marketing_spend
    FROM RAW.FACT_MARKETING_SPEND
    GROUP BY 1,2
)
SELECT
    p.category,
    p.channel,
    p.net_revenue,
    p.contribution_margin,
    s.marketing_spend,
    p.contribution_margin - s.marketing_spend AS net_contribution,
    DIV0(p.net_revenue, s.marketing_spend)    AS romi
FROM cat_pnl p
LEFT JOIN cat_spend s
    ON p.category = s.category AND p.channel = s.channel;

-- ------------------------------------------------------------
-- VIEW 6: Offline store performance
-- Per-store and per-sqft revenue
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW V_STORE_PERFORMANCE AS
SELECT
    s.store_id,
    s.city,
    s.store_type,
    s.sqft,
    COUNT(DISTINCT o.order_id)        AS orders,
    SUM(o.qty)                        AS units,
    SUM(o.gross_revenue)              AS gross_revenue,
    SUM(o.gross_revenue) / s.sqft     AS revenue_per_sqft
FROM RAW.DIM_STORE s
LEFT JOIN RAW.FACT_ORDERS o ON s.store_id = o.store_id
GROUP BY 1,2,3,4;
