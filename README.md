# SNITCH Marketplace Margin & Returns Intelligence System

> A SQL-first marketplace analytics project simulating a multi-channel D2C men's fashion business across Mumbai, Delhi, and Bengaluru. Built end-to-end on **Snowflake + Power BI + Python (Groq / Llama 3)** with an AI workflow layer for natural-language analytics and automated daily insights.

**Built for:** Business Analyst applications (Snitch, ecommerce/retail data roles)
**Author:** Abhishek Parle
**Stack:** Snowflake · Power BI Service · Python · DuckDB · Groq API · Streamlit · GitHub

---

## Live demos

| Demo | URL | What it shows |
|---|---|---|
| Streamlit App | [YOUR_STREAMLIT_APP_URL_HERE](https://YOUR_STREAMLIT_APP_URL_HERE) | AI-powered NL-to-SQL and daily insights, no login required |
| Power BI Dashboard | [YOUR_POWERBI_EMBED_LINK_HERE](https://YOUR_POWERBI_EMBED_LINK_HERE) | 5-page interactive dashboard — P&L, returns, channel, SKU, marketing |

---

## Why this project exists

Snitch's job description for the Business Analyst role is sharp: *"You'll own the numbers that run our marketplace and category business. Daily P&L visibility, pricing models, returns analytics, category performance. Strong SQL and an intent/ability to build AI workflows is non-negotiable."*

This project was built to demonstrate exactly that:
- **Daily P&L visibility** → `V_DAILY_PNL` view + Power BI cockpit page
- **Returns analytics** → category × channel return matrix, "true margin post returns" view exposing bleeding SKUs
- **Pricing models** → channel price parity check across Myntra, Flipkart, Ajio, D2C, Offline
- **Category performance** → marketing efficiency / ROMI scorecard
- **AI workflows** → NL-to-SQL chat over the warehouse + automated daily insights generator

---

## The dataset (mock but realistic)

| Asset | Rows | Notes |
|---|---|---|
| `fact_orders` | ~73,000 | 6 months, Oct 2025 → Mar 2026 |
| `fact_returns` | ~13,000 | 17.9% blended return rate |
| `fact_marketing_spend` | ~5,500 | Daily by channel × category |
| `dim_sku` | 295 | Realistic MRP, COGS, packaging cost |
| `dim_store` | 10 | 3 cities, incl. Bengaluru flagship (10K sqft) |

**Channels:** D2C App, D2C Web, Myntra, Flipkart, Ajio, Offline
**Cities:** Mumbai (38%), Delhi (34%), Bengaluru (28%)
**Categories:** Shirts, T-Shirts, Jeans, Co-ords, Trousers, Jackets

Distributions are tuned to reflect real Indian fast-fashion economics: Co-ords and Jeans return at 25–36% on marketplaces, marketplace commissions of 28–32%, weekend revenue lifts, and higher discount depth on Myntra/Flipkart vs D2C.

---

## What the data revealed (sample insights)

After loading, I ran the analytical views and pulled out three findings I'd take into the interview:

1. **Co-ords on Flipkart return at 36.1%** — vs 12% for T-shirts on D2C App. The post-return margin on this category × channel combo is *negative* despite a healthy gross margin. **Recommendation:** add a size-fit guide on Flipkart PDPs and tighten the SKU assortment to bestsellers.
2. **10 SKUs have negative true margin post-returns** despite showing ₹95K–₹230K in gross margin. They look profitable on the P&L until you net out refunds and reverse logistics. **Recommendation:** auto-flag any SKU where post-return margin drops below ₹0 in the daily ops report.
3. **D2C App is the #1 channel at 29% of revenue**, beating Myntra (22.8%). The 0% commission and lower return rate make it the most profitable channel by a wide margin — **doubling marketing spend on D2C App vs Flipkart yields ~2.4x more contribution rupees.**

---

## Architecture

```
  Python (generate_data.py)
         │
         ▼
   CSVs (data/*.csv)
         │
         ▼
   Snowflake RAW schema  ────►  Snowflake MARTS schema (6 views)
                                          │
                          ┌───────────────┼────────────────┐
                          ▼               ▼                ▼
                     Power BI       Python (DuckDB)   AI Workflow
                     Dashboard      Validation        (Gemini API)
                                                     ├─ NL-to-SQL
                                                     └─ Daily Insights
```

---

## Repo structure

```
snitch_project/
├── data/                       # Generated CSVs
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

---

## How to run it (60 minutes end-to-end)

### Step 1 — Generate the data (2 min)
```bash
pip install pandas numpy faker duckdb
python python/generate_data.py
```

### Step 2 — Spin up Snowflake free trial (10 min)
1. Sign up at https://signup.snowflake.com (30-day free trial, $400 credits, no credit card)
2. Pick AWS Mumbai region for low latency
3. Open a worksheet, paste and run `sql/01_snowflake_ddl.sql`
4. Upload CSVs via Snowsight: **Data → Add Data → Load files into existing tables**
5. Run `sql/02_analytical_views.sql`

### Step 3 — Power BI dashboard (30 min)
Follow `powerbi/DASHBOARD_GUIDE.md` page by page. Connect to Snowflake → MARTS schema → import all 6 views.

### Step 4 — AI workflow layer (10 min)
```bash
pip install groq
export GROQ_API_KEY=<your_free_key>   # https://console.groq.com/keys

# Daily insights generator
python python/ai_workflow.py insights

# Natural language Q&A
python python/ai_workflow.py ask "What is the contribution margin for Co-ords on Flipkart in March?"
```

### Step 5 — Run the Streamlit app locally (2 min)
```bash
pip install streamlit
streamlit run python/streamlit_app.py
```
The app opens at `http://localhost:8501`. Both tabs work without a Groq key (the AI summary falls back to raw JSON). To enable the AI layer, create `.streamlit/secrets.toml`:
```toml
GROQ_API_KEY = "your_groq_api_key_here"
```

### Step 6 — Deploy to Streamlit Community Cloud (5 min)
See the full guide in [docs/STREAMLIT_DEPLOY.md](docs/STREAMLIT_DEPLOY.md).

**Quick steps:**
1. Push this repo to GitHub (public or private)
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Select your repo, branch `main`, main file `python/streamlit_app.py`
4. Click **Advanced settings → Secrets** and paste your `GROQ_API_KEY`
5. Click **Deploy** — live URL in ~2 minutes

### Step 7 — Publish Power BI (5 min)
- Power BI: **Publish** → workspace → **Publish to web** (free, gives you a public link)
- Push repo to GitHub
- Add the Power BI link and Streamlit URL to this README and your resume

---

## Resume bullet (copy-paste ready)

> Built a marketplace margin and returns intelligence system for a simulated Indian D2C fashion brand on **Snowflake + Power BI**, modeling 73K orders across 6 channels and 3 cities. Designed 6 analytical views (daily P&L, returns deep-dive, channel price parity, SKU profitability post-returns, marketing efficiency) and built an AI workflow layer in Python using the **Gemini API** for natural-language SQL queries and automated daily anomaly detection. Surfaced 10 SKUs with negative post-return margin invisible on standard P&L reports.

---

## Cost
**₹0.** Snowflake trial is free. Power BI Service is free for personal use. Gemini API free tier is 1M tokens/day — far beyond what this project needs.
