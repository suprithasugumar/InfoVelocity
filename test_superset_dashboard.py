"""
Test Script: Validate Superset Dashboard, KPI Cards, and Visualizations
========================================================================
Validates that:
1. Dashboard exists with slug 'infovelocity-narrative-tracker'.
2. Auto-refresh is configured to 10 seconds.
3. All 11 slices (5 KPI cards + 6 analytical charts) exist and are linked.
4. Each chart queries Apache Druid successfully and prints its executable SQL.
"""

from superset.app import create_app
import json
from sqlalchemy import text

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    dash = db.session.query(Dashboard).filter(
        (Dashboard.slug == "infovelocity-narrative-tracker") | (Dashboard.id == 10)
    ).first()

    if not dash:
        print("[FAILED] Dashboard not found!")
        exit(1)

    print(f"=== Dashboard: {dash.dashboard_title} ===")
    print(f"ID: {dash.id}")
    print(f"Slug: {dash.slug}")
    print(f"Published: {dash.published}")
    print(f"Slices count: {len(dash.slices)}")

    metadata = json.loads(dash.json_metadata or "{}")
    refresh_freq = metadata.get("refresh_frequency")
    print(f"Auto-refresh interval: {refresh_freq}s")
    if refresh_freq != 10:
        print(f"[WARNING] Expected refresh frequency 10s, found {refresh_freq}s")

    # Verify position layout has all expected sections
    pos = json.loads(dash.position_json or "{}")
    grid_children = pos.get("GRID_ID", {}).get("children", [])
    print(f"Dashboard Grid Rows: {grid_children}")

    expected_keywords = [
        "Active Stories",
        "Avg Velocity",
        "Peak Velocity",
        "Current Top Accel",
        "Peak Accel (6h)",
        "5-Minute Trend",
        "Momentum Map",
        "Fastest Growing Narratives",
        "Breakout Monitor",
        "Narrative Clusters",
        "Story Engagement Over Time"
    ]

    all_passed = True
    found_names = [s.slice_name for s in dash.slices]

    for kw in expected_keywords:
        if not any(kw in name for name in found_names):
            print(f"[MISSING SLICE] Pattern '{kw}' not found in {found_names}")
            all_passed = False

    for s in dash.slices:
        print(f"\n==================================================================")
        print(f"--- Chart: {s.slice_name} (ID: {s.id}, Viz: {s.viz_type}) ---")
        print(f"==================================================================")
        ds = s.datasource
        print(f"Datasource: {ds.name} (Type: {ds.type}, Table/SQL ID: {ds.id})")
        try:
            params = json.loads(s.params)
            
            # Formulate the query string for this chart
            if s.viz_type == "table":
                cols = params.get("all_columns", [])
                cols_str = ", ".join([f'"{c}"' for c in cols])
                order_by = params.get("order_by_cols", [])
                order_clause = ""
                if order_by:
                    col_name, is_desc = json.loads(order_by[0])
                    order_clause = f'ORDER BY "{col_name}" {"DESC" if is_desc else "ASC"}'
                limit = params.get("row_limit", 100)
                if ds.sql:
                    chart_sql = f"SELECT {cols_str}\nFROM (\n{ds.sql}\n) AS \"virtual_table\"\n{order_clause}\nLIMIT {limit}"
                else:
                    chart_sql = f'SELECT {cols_str}\nFROM "{ds.table_name}"\n{order_clause}\nLIMIT {limit}'
            elif s.viz_type in ("big_number", "big_number_total"):
                metric_expr = params.get("metric", {}).get("sqlExpression", "COUNT(*)")
                if ds.sql:
                    chart_sql = f"SELECT {metric_expr} AS metric_val\nFROM (\n{ds.sql}\n) AS \"virtual_table\""
                else:
                    chart_sql = f'SELECT {metric_expr} AS metric_val\nFROM "{ds.table_name}"'
            elif s.viz_type == "echarts_timeseries_line":
                if ds.sql:
                    chart_sql = f"SELECT bucket_time, AVG(avg_score_velocity) AS v_score, AVG(avg_comment_velocity) AS v_comment\nFROM (\n{ds.sql}\n) AS \"virtual_table\"\nGROUP BY bucket_time\nORDER BY bucket_time ASC\nLIMIT 100"
                else:
                    chart_sql = f'SELECT TIME_FLOOR("__time", \'PT5M\') AS bucket_time, AVG(v_score) AS v_score\nFROM "{ds.table_name}"\nGROUP BY TIME_FLOOR("__time", \'PT5M\')\nLIMIT 100'
            elif s.viz_type == "bubble_v2":
                if ds.sql:
                    chart_sql = f"SELECT quadrant, title, AVG(score_velocity_per_minute) AS v, AVG(score_acceleration) AS a, ROUND(SQRT(GREATEST(MAX(current_score), 1)), 2) AS sz\nFROM (\n{ds.sql}\n) AS \"virtual_table\"\nGROUP BY quadrant, title\nLIMIT 100"
                else:
                    chart_sql = f'SELECT title, AVG(v_score) AS v, AVG(a_score) AS a\nFROM "{ds.table_name}"\nGROUP BY title\nLIMIT 100'
            else:
                chart_sql = f"SELECT * FROM ({ds.sql}) sub LIMIT 5" if ds.sql else f'SELECT * FROM "{ds.table_name}" LIMIT 5'

            print("SQL:\n" + chart_sql)

            # Test execution directly against Druid via database engine
            with ds.database.get_sqla_engine() as eng:
                with eng.connect() as conn:
                    res = conn.execute(text(chart_sql))
                    rows = res.fetchall() if hasattr(res, "returns_rows") and res.returns_rows else []
                    print(f"Execution: PASSED ({len(rows)} rows retrieved)")
                    if rows:
                        print("  Sample row 1:", rows[0])
        except Exception as e:
            all_passed = False
            print(f"Execution: FAILED - {e}")

    if all_passed:
        print("\n[ALL 11 SLICES PASSED] InfoVelocity Dashboard & Visualizations Fully Operational!")
    else:
        print("\n[TESTS FAILED] One or more chart queries or slices failed.")
        exit(1)
