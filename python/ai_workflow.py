"""
SNITCH AI Workflow Layer
========================
Two AI-powered workflows:
1. nl_to_sql() - natural language question -> SQL -> result -> plain English answer
2. daily_insights() - pulls yesterday's numbers, detects anomalies, writes exec summary

Uses Groq free tier (14,400 req/day on Llama 3 - more than enough).
Get a free API key at: https://console.groq.com/keys

Set env var:  export GROQ_API_KEY=your_key_here
"""

import os
import json
import duckdb
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# ---- Groq setup (lazy import) ----
# Try Streamlit secrets first (when running inside Streamlit Cloud),
# fall back to the environment variable (when running from the CLI).
# The broad except catches both ImportError (streamlit not installed) and
# StreamlitAPIException (streamlit installed but server not running).
try:
    import streamlit as _st
    API_KEY = _st.secrets.get("GROQ_API_KEY", "") or os.environ.get("GROQ_API_KEY", "")
except Exception:
    API_KEY = os.environ.get("GROQ_API_KEY", "")

CLIENT = None
GROQ_MODEL = "llama-3.3-70b-versatile"
if API_KEY:
    try:
        from groq import Groq
        CLIENT = Groq(api_key=API_KEY)
    except ImportError:
        print("WARN: pip install groq")


def _chat(prompt: str) -> str:
    """Send a single-turn prompt to Groq and return the text response."""
    return CLIENT.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
    ).choices[0].message.content.strip()

# ---- DB connection (DuckDB locally; swap for Snowflake connector in prod) ----
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def get_conn():
    con = duckdb.connect()
    for t in ["dim_sku", "dim_store", "fact_orders", "fact_returns", "fact_marketing_spend"]:
        csv_path = os.path.join(DATA_DIR, f"{t}.csv").replace("\\", "/")
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_csv_auto('{csv_path}')")
    return con

# ---- Schema context for the LLM ----
SCHEMA_CONTEXT = """
You are a SQL analyst for SNITCH, an Indian D2C men's fashion brand.
The database has these tables (DuckDB syntax, similar to Snowflake):

fact_orders(order_id, order_date, city, channel, store_id, sku_id, category,
            mrp, selling_price, discount_pct, qty, gross_revenue, commission,
            logistics_cost, cogs, packaging_cost)
  - cities: 'Mumbai', 'Delhi', 'Bengaluru'
  - channels: 'D2C_APP', 'D2C_WEB', 'MYNTRA', 'FLIPKART', 'AJIO', 'OFFLINE'
  - categories: 'Shirts', 'T-Shirts', 'Jeans', 'Co-ords', 'Trousers', 'Jackets'

fact_returns(return_id, order_id, return_date, return_reason, refund_amount,
             reverse_logistics_cost)
  - join to fact_orders on order_id

fact_marketing_spend(spend_date, channel, category, marketing_spend)
dim_sku(sku_id, category, mrp, cogs, packaging_cost)
dim_store(store_id, city, store_type, sqft, opened_date)

Rules:
- contribution_margin = gross_revenue - refunds - commission - cogs - packaging - logistics - reverse_logistics
- "return rate" = COUNT(returns) / COUNT(orders)
- All amounts in INR. Format large numbers in Lakhs (/100000) or Crores (/10000000).
- ALWAYS return ONLY a SQL query, no markdown fences, no explanation.
"""

# ============================================================
# WORKFLOW 1: NATURAL LANGUAGE -> SQL -> ANSWER
# ============================================================
def nl_to_sql(question: str) -> dict:
    """
    Convert a business question into SQL, run it, and explain the result.
    Returns dict with sql, result_df, explanation.
    """
    if not CLIENT:
        return {"error": "GROQ_API_KEY not set"}

    # Step 1: Generate SQL
    sql_prompt = f"{SCHEMA_CONTEXT}\n\nQuestion: {question}\n\nSQL query:"
    sql = _chat(sql_prompt)
    # Strip markdown fences if model adds them
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Step 2: Execute
    con = get_conn()
    try:
        result = con.execute(sql).fetchdf()
    except Exception as e:
        return {"sql": sql, "error": str(e)}

    # Step 3: Explain in plain English
    # IMPORTANT: tell the LLM the result values are already scaled by the SQL
    # (e.g. divided by 1e7 → already in Crores) so it must NOT re-scale them.
    explain_prompt = f"""
Question asked: {question}
SQL run: {sql}
Result (first 20 rows):
{result.head(20).to_string()}

Rules for your answer:
- The numeric values above are EXACTLY as computed by the SQL — do NOT re-scale them.
  If the SQL divided by 10000000 the column is already in Crores; if by 100000 it is already in Lakhs.
  Use the numbers as-is, just add the correct unit label (Cr / Lakh / %) from the column name or SQL.
- Write 2-3 sentences in plain English for a business stakeholder.
- Be specific: quote the exact number from the result, then interpret it.
- No preamble, no "Based on the data…", just the answer.
"""
    explanation = _chat(explain_prompt)

    return {"sql": sql, "result": result, "explanation": explanation}


# ============================================================
# WORKFLOW 2: DAILY INSIGHTS GENERATOR
# ============================================================
def daily_insights(target_date: str = None) -> str:
    """
    Pull yesterday's numbers, compare to 7-day avg, detect anomalies,
    write a 5-bullet exec summary.
    """
    con = get_conn()

    # Default to last day in dataset
    if not target_date:
        target_date = con.execute("SELECT MAX(order_date) FROM fact_orders").fetchone()[0]

    # Pull KPIs for target day vs 7-day prior average
    metrics = con.execute(f"""
        WITH daily AS (
            SELECT order_date,
                   SUM(gross_revenue) AS revenue,
                   COUNT(DISTINCT order_id) AS orders,
                   SUM(commission) AS commission,
                   SUM(cogs) AS cogs,
                   SUM(packaging_cost) AS pack,
                   SUM(logistics_cost) AS logistics
            FROM fact_orders GROUP BY 1
        ),
        rets AS (
            SELECT o.order_date,
                   SUM(r.refund_amount) AS refunds,
                   SUM(r.reverse_logistics_cost) AS rev_log
            FROM fact_returns r JOIN fact_orders o USING(order_id)
            GROUP BY 1
        )
        SELECT d.order_date, d.revenue, d.orders,
               COALESCE(r.refunds,0) AS refunds,
               d.revenue - COALESCE(r.refunds,0) - d.commission - d.cogs - d.pack - d.logistics - COALESCE(r.rev_log,0) AS contribution_margin
        FROM daily d LEFT JOIN rets r USING(order_date)
        WHERE d.order_date BETWEEN DATE '{target_date}' - INTERVAL '7 days' AND DATE '{target_date}'
        ORDER BY d.order_date
    """).fetchdf()

    today = metrics.iloc[-1]
    prior_avg = metrics.iloc[:-1].mean(numeric_only=True)

    # Top movers - category contribution today vs prior avg
    cat_mix = con.execute(f"""
        SELECT category,
               SUM(CASE WHEN order_date = DATE '{target_date}' THEN gross_revenue ELSE 0 END) AS today_rev,
               SUM(CASE WHEN order_date BETWEEN DATE '{target_date}' - INTERVAL '7 days'
                                              AND DATE '{target_date}' - INTERVAL '1 day'
                        THEN gross_revenue ELSE 0 END) / 7.0 AS avg_rev
        FROM fact_orders GROUP BY 1
    """).fetchdf()
    cat_mix["delta_pct"] = (cat_mix["today_rev"] - cat_mix["avg_rev"]) / cat_mix["avg_rev"] * 100

    # Top return reasons today
    top_return_reasons = con.execute(f"""
        SELECT return_reason, COUNT(*) AS n
        FROM fact_returns WHERE return_date = DATE '{target_date}'
        GROUP BY 1 ORDER BY n DESC LIMIT 3
    """).fetchdf()

    payload = {
        "date": str(target_date),
        "revenue_today_lakhs": round(today["revenue"]/1e5, 2),
        "revenue_7d_avg_lakhs": round(prior_avg["revenue"]/1e5, 2),
        "orders_today": int(today["orders"]),
        "orders_7d_avg": int(prior_avg["orders"]),
        "contribution_margin_today_lakhs": round(today["contribution_margin"]/1e5, 2),
        "contribution_margin_7d_avg_lakhs": round(prior_avg["contribution_margin"]/1e5, 2),
        "category_movers": cat_mix.to_dict("records"),
        "top_return_reasons_today": top_return_reasons.to_dict("records"),
    }

    if not CLIENT:
        return json.dumps(payload, indent=2, default=str)

    prompt = f"""
You are a business analyst for SNITCH writing the daily ops summary email.
Here is the data for {target_date} vs the prior 7-day average:

{json.dumps(payload, indent=2, default=str)}

Write exactly 5 bullets:
1. Headline (revenue + margin vs 7d avg, with % change)
2. Best performing category today
3. Worst performing category today
4. Top return reason concern (if any spike)
5. One specific recommended action

Be concrete. Use INR Lakhs/Crores. No fluff. Each bullet under 25 words.
"""
    summary = _chat(prompt)
    return summary


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "insights":
        print(daily_insights())
    elif len(sys.argv) > 1 and sys.argv[1] == "ask":
        question = " ".join(sys.argv[2:])
        result = nl_to_sql(question)
        print("\n--- SQL ---")
        print(result.get("sql"))
        print("\n--- Result ---")
        print(result.get("result"))
        print("\n--- Answer ---")
        print(result.get("explanation"))
    else:
        print("Usage:")
        print("  python ai_workflow.py insights")
        print("  python ai_workflow.py ask 'What was Co-ords return rate on Flipkart?'")
