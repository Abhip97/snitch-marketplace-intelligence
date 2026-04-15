import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

con = duckdb.connect()
for t in ["dim_sku", "dim_store", "fact_orders", "fact_returns", "fact_marketing_spend"]:
    csv_path = os.path.join(DATA_DIR, f"{t}.csv").replace("\\", "/")
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_csv_auto('{csv_path}')")
    count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {count:,} rows")

print()
print(con.execute(
    "SELECT channel, ROUND(SUM(gross_revenue)/1e7, 2) AS revenue_cr "
    "FROM fact_orders GROUP BY 1 ORDER BY revenue_cr DESC"
).fetchdf())
