"""
SNITCH Analytics — AI Workflow  |  Streamlit front-end
=======================================================
Three-tab browser UI:
  • Dashboard      — Visual overview with glassmorphic charts
  • Daily Insights — AI-generated 5-bullet executive summary
  • Ask a Question — Natural-language Q&A (NL → SQL → answer)

Run locally:  streamlit run python/streamlit_app.py
Deploy:       See docs/STREAMLIT_DEPLOY.md
"""

import os
import sys
import pandas as pd
import streamlit as st
import altair as alt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ai_workflow import daily_insights, nl_to_sql, get_conn  # noqa: E402

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SNITCH Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Glassmorphic CSS ─────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Full-app gradient background ── */
.stApp {
    background: linear-gradient(135deg, #060b18 0%, #0d1b3e 50%, #1a0a2e 100%);
    background-attachment: fixed;
}

/* ── Metric cards — frosted glass ── */
[data-testid="metric-container"] {
    background: rgba(255,255,255,0.06);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border: 1px solid rgba(255,255,255,0.13);
    border-radius: 16px;
    padding: 22px 26px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.10);
    transition: transform .15s ease, box-shadow .15s ease;
}
[data-testid="metric-container"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 12px 40px rgba(99,102,241,0.25), inset 0 1px 0 rgba(255,255,255,0.12);
}

/* ── Altair chart wrappers — glass panel ── */
[data-testid="stArrowVegaLiteChart"] {
    background: rgba(255,255,255,0.04);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid rgba(255,255,255,0.09);
    border-radius: 16px;
    padding: 14px 10px 6px;
}

/* ── Sidebar glass ── */
[data-testid="stSidebar"] > div:first-child {
    background: rgba(10,15,40,0.75);
    backdrop-filter: blur(24px);
    -webkit-backdrop-filter: blur(24px);
    border-right: 1px solid rgba(255,255,255,0.08);
}

/* ── Tab bar ── */
.stTabs [data-baseweb="tab-list"] {
    background: rgba(255,255,255,0.04);
    border-radius: 14px;
    padding: 5px;
    gap: 4px;
    border: 1px solid rgba(255,255,255,0.08);
}
.stTabs [data-baseweb="tab"] {
    border-radius: 10px;
    color: #94a3b8;
    padding: 8px 20px;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: rgba(99,102,241,0.28) !important;
    color: #c7d2fe !important;
    border: 1px solid rgba(99,102,241,0.45) !important;
}

/* ── Buttons ── */
.stButton > button, .stLinkButton > a {
    background: rgba(99,102,241,0.18) !important;
    border: 1px solid rgba(99,102,241,0.40) !important;
    border-radius: 10px !important;
    color: #c7d2fe !important;
    backdrop-filter: blur(8px);
    transition: all .15s ease;
}
.stButton > button:hover, .stLinkButton > a:hover {
    background: rgba(99,102,241,0.35) !important;
    border-color: rgba(99,102,241,0.65) !important;
    box-shadow: 0 0 16px rgba(99,102,241,0.3) !important;
}
/* Primary button — glowing accent */
.stButton > button[kind="primary"] {
    background: rgba(99,102,241,0.45) !important;
    border-color: rgba(99,102,241,0.70) !important;
    box-shadow: 0 0 20px rgba(99,102,241,0.25);
}

/* ── Expanders ── */
details {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.09) !important;
    border-radius: 12px !important;
    padding: 2px 14px 2px !important;
    backdrop-filter: blur(8px);
}
details summary { margin-bottom: 4px; }

/* ── Text input ── */
[data-baseweb="input"] > div {
    background: rgba(255,255,255,0.06) !important;
    border: 1px solid rgba(255,255,255,0.14) !important;
    border-radius: 10px !important;
    backdrop-filter: blur(8px);
}

/* ── Divider ── */
hr { border-color: rgba(255,255,255,0.08) !important; }

/* ── st.success / st.info / st.error ── */
[data-testid="stAlert"] {
    border-radius: 12px !important;
    backdrop-filter: blur(8px);
}
</style>
""", unsafe_allow_html=True)


# ── Cached helpers ────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def _conn():
    """Shared DuckDB connection reused across reruns (not JSON-serialised)."""
    return get_conn()


@st.cache_data(show_spinner=False)
def _max_date():
    return _conn().execute("SELECT MAX(order_date) FROM fact_orders").fetchone()[0]


@st.cache_data(show_spinner=False)
def _dashboard_data() -> dict:
    """Load all dashboard data once; returns dict of DataFrames (cache-serialisable)."""
    con = get_conn()

    # ── Top-level KPIs ────────────────────────────────────────────────────────
    o = con.execute("""
        SELECT SUM(gross_revenue) AS rev, COUNT(DISTINCT order_id) AS orders,
               SUM(commission)+SUM(cogs)+SUM(packaging_cost)+SUM(logistics_cost) AS costs
        FROM fact_orders
    """).fetchone()
    r = con.execute("""
        SELECT COUNT(*) AS n,
               COALESCE(SUM(refund_amount + reverse_logistics_cost), 0) AS ret_cost
        FROM fact_returns
    """).fetchone()
    kpis = dict(
        revenue=o[0], orders=int(o[1]),
        return_rate=r[0] / o[1] * 100,
        cm_pct=(o[0] - r[1] - o[2]) / o[0] * 100,
    )

    # ── Monthly revenue trend ─────────────────────────────────────────────────
    monthly = con.execute("""
        SELECT DATE_TRUNC('month', order_date) AS month,
               SUM(gross_revenue) / 1e5        AS revenue_l,
               COUNT(DISTINCT order_id)         AS orders
        FROM fact_orders GROUP BY 1 ORDER BY 1
    """).fetchdf()
    # Pre-format labels in Python so Altair encodes them as nominal (N),
    # guaranteeing exactly one tick per data point with no duplicates.
    monthly["month_label"] = monthly["month"].dt.strftime("%b '%y")
    monthly_order = monthly["month_label"].tolist()   # preserves chronological sort

    # ── Revenue by channel ────────────────────────────────────────────────────
    channel = con.execute("""
        SELECT channel,
               SUM(gross_revenue) / 1e5               AS revenue_l,
               SUM(commission) / SUM(gross_revenue) * 100 AS commission_pct
        FROM fact_orders GROUP BY 1 ORDER BY revenue_l DESC
    """).fetchdf()

    # ── Return rate by category ───────────────────────────────────────────────
    cat_ret = con.execute("""
        SELECT o.category,
               COUNT(DISTINCT r.return_id) * 100.0 / COUNT(DISTINCT o.order_id) AS return_rate,
               COUNT(DISTINCT o.order_id) AS orders
        FROM fact_orders o
        LEFT JOIN fact_returns r ON o.order_id = r.order_id
        GROUP BY 1 ORDER BY return_rate DESC
    """).fetchdf()

    # ── Contribution margin % by channel (post-returns) ───────────────────────
    ch_cm = con.execute("""
        SELECT o.channel,
               ROUND(
                   (SUM(o.gross_revenue)
                    - COALESCE(SUM(r.refund_amount), 0)
                    - SUM(o.commission) - SUM(o.cogs)
                    - SUM(o.packaging_cost) - SUM(o.logistics_cost)
                    - COALESCE(SUM(r.reverse_logistics_cost), 0)
                   ) / SUM(o.gross_revenue) * 100, 1) AS cm_pct
        FROM fact_orders o
        LEFT JOIN fact_returns r ON o.order_id = r.order_id
        GROUP BY 1 ORDER BY cm_pct DESC
    """).fetchdf()
    # Label for diverging color encoding
    ch_cm["sign"] = ch_cm["cm_pct"].apply(lambda x: "Profitable" if x >= 0 else "Loss-making")

    # ── Top 5 return reasons (donut) ──────────────────────────────────────────
    reasons = con.execute("""
        SELECT return_reason, COUNT(*) AS n
        FROM fact_returns GROUP BY 1 ORDER BY n DESC LIMIT 5
    """).fetchdf()

    # ── Revenue by city (donut) ───────────────────────────────────────────────
    city = con.execute("""
        SELECT city, SUM(gross_revenue) / 1e7 AS revenue_cr
        FROM fact_orders GROUP BY 1 ORDER BY revenue_cr DESC
    """).fetchdf()

    return dict(kpis=kpis, monthly=monthly, channel=channel,
                cat_ret=cat_ret, ch_cm=ch_cm, reasons=reasons, city=city)


# ── Altair dark-theme helper — applied once per chart before st.altair_chart ─
def _styled(chart, h: int = 290):
    """Wrap chart in consistent dark glassmorphic Altair theme."""
    return (
        chart.properties(height=h)
        .configure(background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor="#94a3b8", titleColor="#94a3b8",
            gridColor="rgba(255,255,255,0.06)",
            domainColor="rgba(255,255,255,0.15)",
            tickColor="rgba(255,255,255,0.10)",
        )
        .configure_legend(
            labelColor="#94a3b8", titleColor="#94a3b8",
            padding=8, cornerRadius=8,
        )
        .configure_title(color="#e2e8f0")
    )


# ── Header ────────────────────────────────────────────────────────────────────
st.title("SNITCH Marketplace Intelligence")
st.caption("Margin · Returns · Channel Analytics  ·  73K orders  ·  6 channels  ·  3 cities  ·  6 months")
col_h, _ = st.columns([1, 9])
col_h.link_button("GitHub Repo", "https://github.com/YOUR_USERNAME/snitch_project")
st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("About this project")
    st.write(
        "Portfolio project simulating SNITCH's multi-channel marketplace analytics. "
        "Models ~73K orders across D2C App, D2C Web, Myntra, Flipkart, Ajio, and Offline "
        "in Mumbai, Delhi, and Bengaluru over 6 months. "
        "The AI layer (Groq / Llama 3) answers questions and writes daily ops summaries."
    )
    st.subheader("Stack")
    st.markdown(
        "- **Snowflake** — cloud data warehouse\n"
        "- **Python + DuckDB** — local analytics\n"
        "- **Groq / Llama 3** — AI layer\n"
        "- **Streamlit + Altair** — this web app"
    )
    st.subheader("Sample questions")
    SAMPLES = [
        "Which category has the highest return rate on Flipkart?",
        "What is the contribution margin for Co-ords on Myntra in March?",
        "Show top 5 SKUs by gross revenue in February.",
        "Which channel has the lowest commission as % of revenue?",
        "What are the top 3 return reasons for Jeans across all channels?",
    ]
    for q in SAMPLES:
        if st.button(q, use_container_width=True, key=f"s|{q}"):
            st.session_state["q_input"] = q

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊  Dashboard", "📅  Daily Insights", "💬  Ask a Question"])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    with st.spinner("Loading dashboard…"):
        d = _dashboard_data()
    k = d["kpis"]

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Revenue",            f"₹{k['revenue']/1e7:.2f} Cr")
    k2.metric("Total Orders",             f"{k['orders']:,}")
    k3.metric("Blended Return Rate",      f"{k['return_rate']:.1f}%")
    k4.metric("True Contribution Margin", f"{k['cm_pct']:.1f}%",
              help="(Revenue − refunds − commission − COGS − all logistics) / Revenue")

    st.markdown("&nbsp;")

    # ── Row 1: Monthly revenue trend (wide) + Channel revenue (narrow) ────────
    rc1, rc2 = st.columns([5, 3])

    with rc1:
        st.markdown("#### Monthly Revenue Trend")
        # Use N (nominal) + explicit sort so Altair emits exactly one label
        # per month with no auto-generated duplicates from temporal scaling.
        month_order = d["monthly"]["month_label"].tolist()
        base = alt.Chart(d["monthly"]).encode(
            x=alt.X("month_label:N", sort=month_order, title=None,
                    axis=alt.Axis(labelAngle=-30, grid=False)),
            y=alt.Y("revenue_l:Q", title="Revenue (₹ Lakhs)",
                    axis=alt.Axis(format=",.0f")),
            tooltip=[
                alt.Tooltip("month_label:N", title="Month"),
                alt.Tooltip("revenue_l:Q",   title="₹ Lakhs", format=",.1f"),
                alt.Tooltip("orders:Q",      title="Orders",  format=","),
            ],
        )
        trend = (
            base.mark_area(color="#6366f1", opacity=0.18)
            + base.mark_line(color="#818cf8", strokeWidth=2.5)
            + base.mark_point(color="#a5b4fc", size=70, filled=True)
        )
        st.altair_chart(_styled(trend, h=280), use_container_width=True)

    with rc2:
        st.markdown("#### Revenue by Channel")
        ch_order = d["channel"]["channel"].tolist()
        ch_bar = alt.Chart(d["channel"]).mark_bar(
            cornerRadiusTopRight=5, cornerRadiusBottomRight=5
        ).encode(
            x=alt.X("revenue_l:Q", title="₹ Lakhs",
                    axis=alt.Axis(format=",.0f", grid=False)),
            y=alt.Y("channel:N", sort=ch_order, title=None),
            color=alt.Color("channel:N",
                            scale=alt.Scale(scheme="tableau10"), legend=None),
            tooltip=[
                alt.Tooltip("channel:N",        title="Channel"),
                alt.Tooltip("revenue_l:Q",      title="₹ Lakhs",     format=",.1f"),
                alt.Tooltip("commission_pct:Q", title="Commission %", format=".1f"),
            ],
        )
        st.altair_chart(_styled(ch_bar, h=280), use_container_width=True)

    # ── Row 2: Category return rate + CM% diverging bar ───────────────────────
    rc3, rc4 = st.columns(2)

    with rc3:
        st.markdown("#### Return Rate by Category  *(higher = margin erosion)*")
        cat_order = d["cat_ret"]["category"].tolist()
        cat_bar = alt.Chart(d["cat_ret"]).mark_bar(
            cornerRadiusTopRight=5, cornerRadiusBottomRight=5
        ).encode(
            x=alt.X("return_rate:Q", title="Return Rate (%)",
                    axis=alt.Axis(format=".1f", grid=False)),
            y=alt.Y("category:N", sort=cat_order, title=None),
            color=alt.Color("return_rate:Q",
                            scale=alt.Scale(scheme="reds", domain=[10, 32]),
                            legend=None),
            tooltip=[
                alt.Tooltip("category:N",    title="Category"),
                alt.Tooltip("return_rate:Q", title="Return Rate %", format=".2f"),
                alt.Tooltip("orders:Q",      title="Total Orders",  format=","),
            ],
        )
        st.altair_chart(_styled(cat_bar), use_container_width=True)

    with rc4:
        st.markdown("#### Contribution Margin % by Channel  *(post-returns)*")
        cm_order = d["ch_cm"]["channel"].tolist()
        # Diverging bar: green = profitable, red = loss-making
        cm_bars = alt.Chart(d["ch_cm"]).mark_bar(
            cornerRadiusTopRight=5, cornerRadiusBottomRight=5,
            cornerRadiusTopLeft=5, cornerRadiusBottomLeft=5,
        ).encode(
            x=alt.X("cm_pct:Q", title="CM %",
                    scale=alt.Scale(domain=[-30, 55]),
                    axis=alt.Axis(format=".0f", grid=True)),
            y=alt.Y("channel:N", sort=cm_order, title=None),
            color=alt.Color("sign:N",
                            scale=alt.Scale(
                                domain=["Profitable", "Loss-making"],
                                range=["#22c55e", "#ef4444"]),
                            legend=alt.Legend(title=None, orient="bottom")),
            tooltip=[
                alt.Tooltip("channel:N", title="Channel"),
                alt.Tooltip("cm_pct:Q", title="CM %",  format=".1f"),
                alt.Tooltip("sign:N",    title="Status"),
            ],
        )
        # Zero reference line so negative bars are clearly anchored
        zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(
            color="rgba(255,255,255,0.35)", strokeWidth=1.5, strokeDash=[4, 3]
        ).encode(x=alt.X("x:Q"))
        st.altair_chart(_styled(cm_bars + zero), use_container_width=True)

    # ── Row 3: Return reasons donut + Revenue by city donut ───────────────────
    rc5, rc6 = st.columns(2)

    with rc5:
        st.markdown("#### Top Return Reasons")
        # Abbreviate long labels for the legend
        reasons_df = d["reasons"].copy()
        reasons_df["short_reason"] = reasons_df["return_reason"].str.replace(
            "not as expected", "≠ expected", regex=False
        )
        donut_r = alt.Chart(reasons_df).mark_arc(
            innerRadius=65, outerRadius=125, padAngle=0.02, cornerRadius=4
        ).encode(
            theta=alt.Theta("n:Q", stack=True),
            color=alt.Color("short_reason:N",
                            scale=alt.Scale(scheme="tableau10"),
                            legend=alt.Legend(title=None, orient="bottom",
                                              columns=2, symbolSize=120)),
            tooltip=[
                alt.Tooltip("return_reason:N", title="Reason"),
                alt.Tooltip("n:Q",             title="Returns", format=","),
            ],
        )
        st.altair_chart(_styled(donut_r, h=310), use_container_width=True)

    with rc6:
        st.markdown("#### Revenue by City")
        CITY_COLORS = {"Mumbai": "#6366f1", "Delhi": "#22d3ee", "Bengaluru": "#f59e0b"}
        city_df = d["city"].copy()
        city_df["color"] = city_df["city"].map(CITY_COLORS)
        donut_c = alt.Chart(city_df).mark_arc(
            innerRadius=65, outerRadius=125, padAngle=0.02, cornerRadius=4
        ).encode(
            theta=alt.Theta("revenue_cr:Q", stack=True),
            color=alt.Color("city:N",
                            scale=alt.Scale(
                                domain=list(CITY_COLORS.keys()),
                                range=list(CITY_COLORS.values())),
                            legend=alt.Legend(title=None, orient="bottom",
                                              symbolSize=120)),
            tooltip=[
                alt.Tooltip("city:N",        title="City"),
                alt.Tooltip("revenue_cr:Q",  title="₹ Crores", format=".2f"),
            ],
        )
        st.altair_chart(_styled(donut_c, h=310), use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — DAILY INSIGHTS
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Daily Ops Summary")
    st.write("Pick a date to generate an AI-written 5-bullet executive summary.")
    max_dt   = _max_date()
    sel_date = st.date_input("Date", value=max_dt, max_value=max_dt)

    if st.button("Generate Summary", type="primary"):
        with st.spinner("Querying data and generating summary…"):
            summary = daily_insights(target_date=str(sel_date))
        with st.container(border=True):
            st.markdown(summary)

        kpi_df = _conn().execute(f"""
            WITH d AS (
                SELECT order_date,
                       SUM(gross_revenue) AS rev, COUNT(DISTINCT order_id) AS orders,
                       SUM(commission)+SUM(cogs)+SUM(packaging_cost)+SUM(logistics_cost) AS costs
                FROM fact_orders GROUP BY 1
            ),
            r AS (
                SELECT o.order_date,
                       SUM(rt.refund_amount)+SUM(rt.reverse_logistics_cost) AS ret_cost
                FROM fact_returns rt JOIN fact_orders o USING(order_id) GROUP BY 1
            )
            SELECT d.order_date, d.rev, d.orders,
                   d.rev - COALESCE(r.ret_cost, 0) - d.costs AS cm
            FROM d LEFT JOIN r USING(order_date)
            WHERE d.order_date
                  BETWEEN DATE '{sel_date}' - INTERVAL '7 days' AND DATE '{sel_date}'
            ORDER BY 1
        """).fetchdf()
        today, avg7 = kpi_df.iloc[-1], kpi_df.iloc[:-1].mean(numeric_only=True)

        st.subheader("KPI Snapshot")
        m1, m2, m3 = st.columns(3)
        m1.metric("Revenue",             f"₹{today['rev']/1e5:.1f} L",
                  f"{(today['rev']-avg7['rev'])/avg7['rev']*100:+.1f}% vs 7d avg")
        m2.metric("Orders",              f"{int(today['orders']):,}",
                  f"{(today['orders']-avg7['orders'])/avg7['orders']*100:+.1f}% vs 7d avg")
        m3.metric("Contribution Margin", f"₹{today['cm']/1e5:.1f} L",
                  f"{(today['cm']-avg7['cm'])/avg7['cm']*100:+.1f}% vs 7d avg")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — ASK A QUESTION
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Natural Language Q&A")
    st.write(
        "Ask any business question in plain English — the AI generates SQL, "
        "runs it against the dataset, and explains the result."
    )
    question = st.text_input(
        "Your question",
        placeholder="Which category has the highest return rate on Flipkart?",
        key="q_input",
    )
    if st.button("Ask", type="primary"):
        if not question.strip():
            st.warning("Please enter a question before clicking Ask.")
        else:
            with st.spinner("Generating SQL and running query…"):
                res = nl_to_sql(question)
            if "error" in res and "sql" not in res:
                st.error(
                    f"**Setup error:** {res['error']}\n\n"
                    "Add `GROQ_API_KEY` to `.streamlit/secrets.toml` (local) "
                    "or to the Streamlit Cloud secrets UI (deployed)."
                )
            else:
                with st.expander("Generated SQL", expanded=True):
                    st.code(res.get("sql", ""), language="sql")
                if "error" in res:
                    st.error(f"**SQL execution failed:** {res['error']}\n\nTry rephrasing.")
                else:
                    with st.expander("Result", expanded=True):
                        st.dataframe(res["result"], use_container_width=True)
                    with st.expander("Answer", expanded=True):
                        st.success(res.get("explanation", "No explanation returned."))
