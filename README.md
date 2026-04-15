# SNITCH Marketplace Margin & Returns Intelligence System

A marketplace analytics system that surfaces the true profitability of a multi-channel D2C fashion business across own platforms, third-party marketplaces, and offline retail at the granularity needed to make day-to-day commercial decisions.

**Author:** Abhishek Parle

**Stack:** Snowflake · Power BI Service · Python · DuckDB · Groq API · Streamlit · GitHub

---

## Live demos

## Streamlit App: https://snitch-marketplace-intelligence.streamlit.app/

<img width="1850" height="866" alt="image" src="https://github.com/user-attachments/assets/f844545c-3495-4dce-bcf4-1b67d33e2e99" />


<img width="1902" height="1012" alt="image" src="https://github.com/user-attachments/assets/0712d32e-af87-4ff2-83c0-b0d892f1920b" />


## Power BI Dashboard:

<img width="1252" height="702" alt="image" src="https://github.com/user-attachments/assets/27b3a277-bf38-4727-9257-eb056e6bbcbf" />





---

## Why this project exists

Marketplace-driven D2C fashion is a category where standard P&L reports actively hide the real economics. Gross margin on a SKU can look healthy while post-return margin is negative once refunds, reverse logistics, and marketplace commissions are netted out. A category-channel combination can show strong revenue while contributing nothing to the bottom line. The fast-fashion playbook of "launch many, learn fast" only works if the analytics layer can keep up — and most of it lives in spreadsheets pulled together at the end of each month.

This project is a working prototype of what that analytics layer looks like when built right: daily granularity, every cost line accounted for, the bleeding SKUs surfaced automatically, and a natural-language interface so anyone in the business can interrogate the numbers without writing SQL.

What's in it:
- **Daily P&L visibility** — channel × city × category × day, with a full cost waterfall from gross revenue down to contribution margin
- **Returns analytics** — return rate matrix by category × channel, return-reason breakdown, and a true-margin-post-returns view that exposes SKUs silently bleeding profitability
- **Pricing analytics** — channel price parity check across D2C, Myntra, Flipkart, Ajio, and offline stores to detect SKUs underpriced on one channel and cannibalising margin on another
- **Category performance** — marketing efficiency / ROMI scorecard joining contribution margin to ad spend per category × channel
- **AI workflows** — natural-language SQL chat over the warehouse, plus an automated daily insights generator that detects anomalies and writes a five-bullet executive summary

---

## Architecture

The system is built on a four-layer stack: a Snowflake data warehouse, a SQL semantic layer, a Power BI presentation layer, and a Python service layer for AI-driven workflows.

**Data warehouse — Snowflake**
A two-schema design separates raw and modelled data. The `RAW` schema holds five tables — `FACT_ORDERS`, `FACT_RETURNS`, `FACT_MARKETING_SPEND`, `DIM_SKU`, `DIM_STORE` — modelled as a star schema with conformed dimensions on city, channel, category, and SKU. The `MARTS` schema holds the analytical views that encode business logic, keeping raw data immutable and the transformation layer version-controlled in SQL.

**Semantic layer — SQL views**
Seven views in `MARTS` translate raw events into business metrics:

| View | Purpose |
|---|---|
| `V_DAILY_PNL` | Channel × city × category × day with full cost waterfall (revenue → discount → returns → commission → COGS → packaging → logistics → contribution margin) |
| `V_RETURNS_ANALYTICS` | Return reason concentration by category, channel, and city |
| `V_RETURN_RATE` | Return rate matrix at category × channel grain |
| `V_PRICE_PARITY` | Average selling price per SKU across every channel, with spread |
| `V_SKU_PROFITABILITY` | Gross margin vs true margin post-returns per SKU |
| `V_MARKETING_EFFICIENCY` | Contribution margin and ROMI joined to spend per category × channel |
| `V_STORE_PERFORMANCE` | Per-store and per-square-foot economics for offline retail |

The contribution margin definition lives in one place — change it once, every downstream consumer updates.

**Presentation layer — Power BI**
A five-page Power BI report connected to the MARTS schema. The Overview page is a single-screen executive cockpit — Online/Offline channel toggle, city filter, hero KPI, daily contribution margin trend with seven-day rolling average, channel waterfall, return-rate heatmap, and a live "bleeding SKUs" table. Detail pages cover returns, pricing parity, marketing efficiency, and store performance. The dashboard is published to Power BI Service and embedded for public access.

**AI workflow layer — Python**
Two production-style workflows wrap the warehouse:

1. **Natural-language SQL.** Accepts a business question, prompts a frontier LLM with the schema context and business rules, generates SQL, executes it, and returns the SQL, the result, and a plain-English explanation. The LLM never sees the underlying data — only schema metadata and the query result the user already has access to.

2. **Automated daily insights.** Computes deterministic KPIs (yesterday's revenue, contribution margin, category movers, return-reason concentration vs trailing seven-day baselines), passes the structured payload to the LLM purely for narration, and emits a five-bullet executive summary. The pattern deliberately separates compute (deterministic SQL) from narration (creative LLM) so numbers are never hallucinated.

A Streamlit web app exposes both workflows through a browser interface, deployed via GitHub-driven continuous deployment with secrets managed through the platform's encrypted store.

---

## Findings

Three findings drawn from the modelled data, representative of the kind of insight the system is built to surface:

**Co-ords on Flipkart return at 36.1%.** Compared with 12% for T-shirts on the D2C app. Despite a healthy gross margin, the post-return margin on this combination is negative once refunds and reverse logistics are netted out. Action: tighten the SKU assortment on Flipkart to bestsellers, add a size-fit guide on the PDP, and flag the channel-category for assortment review at the next merchandising cycle.

**Ten SKUs show negative true margin post-returns.** These SKUs report ₹95K to ₹230K in gross margin but post negative margin once refunds and reverse logistics are deducted. They are invisible on standard P&L dashboards that stop at gross. Action: auto-flag any SKU where post-return margin drops below zero in the daily ops report; consider price corrections, fit improvements, or delisting for the worst offenders.

**D2C App is the most profitable channel.** It contributes 29% of gross revenue against Myntra at 22.8%, but with zero marketplace commission and lower returns, it generates roughly 2.4x more contribution rupees per rupee of marketing spend. Action: rebalance incremental marketing budget from Flipkart and Ajio toward D2C App, and consider channel-specific creative that drives app installs.

---

## Stack

Snowflake (data warehouse) · Power BI Service (BI and distribution) · Python 3.14 (orchestration and AI layer) · DuckDB (local query engine for the AI workflow) · Graq llama-3.3-70b-versatile (LLM) · Streamlit Community Cloud (web deployment) · GitHub (source control and continuous deployment)

---

## Repository structure

```
snitch_project/
├── data/                       # CSVs
│   ├── dim_sku.csv
│   ├── dim_store.csv
│   ├── fact_orders.csv
│   ├── fact_returns.csv
│   └── fact_marketing_spend.csv
├── python/
│   ├── generate_data.py        # Mock data generator (Faker + numpy)
│   └── ai_workflow.py          # NL-to-SQL + daily insights (Gemini)
├── sql/
│   ├── 01_snowflake_ddl.sql    # Schema, tables, COPY commands
│   └── 02_analytical_views.sql # 6 marts views powering the dashboard
├── powerbi/
│   └── DASHBOARD_GUIDE.md      # Page-by-page build guide
└── docs/
    └── case_study.pdf          # 1-page recruiter handout
```

