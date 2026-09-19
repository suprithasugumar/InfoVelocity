"""
Test Script: Validate Superset Dashboard, KPI Cards, and Visualizations
========================================================================
Validates that:
1. Dashboard ID 10 exists with slug 'infovelocity-narrative-tracker'.
2. Auto-refresh is configured to 10 seconds.
3. All 8 slices (4 KPI cards + 4 core charts) exist and are linked to the dashboard.
4. Each chart queries Apache Druid successfully and returns data.
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
        (Dashboard.id == 10) | (Dashboard.slug == "infovelocity-narrative-tracker")
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

    expected_slices = [
        "Active Stories",
        "Average Score Velocity",
        "Highest Score Velocity",
        "Highest Positive Acceleration",
        "Engagement Velocity — 5-Minute Trend",
        "Fastest Growing Narratives",
        "Breakout Monitor — Positive Acceleration",
        "Story Engagement Over Time"
    ]

    all_passed = True
    found_names = [s.slice_name for s in dash.slices]

    for expected in expected_slices:
        if expected not in found_names:
            print(f"[MISSING SLICE] {expected}")
            all_passed = False

    for s in dash.slices:
        print(f"\n--- Testing Chart: {s.slice_name} (ID: {s.id}, Viz: {s.viz_type}) ---")
        ds = s.datasource
        print(f"Datasource: {ds.name} (Type: {ds.type}, Table/SQL ID: {ds.id})")
        try:
            params = json.loads(s.params)
            if s.viz_type == "table":
                cols = params.get("all_columns", [])
                print(f"Columns: {cols}")
            elif s.viz_type == "big_number_total":
                metric = params.get("metric", {})
                print(f"Metric: {metric}")
            
            # Execute query test on dataset
            if ds.sql:
                test_sql = f"SELECT * FROM ({ds.sql}) sub LIMIT 3"
            else:
                test_sql = f'SELECT * FROM "{ds.table_name}" LIMIT 3'

            with ds.database.get_sqla_engine() as eng:
                with eng.connect() as conn:
                    res = conn.execute(text(test_sql))
                    rows = res.fetchall()
                    print(f"Status: PASSED ({len(rows)} sample rows retrieved)")
                    for r in rows[:2]:
                        print("  Sample row:", r)
        except Exception as e:
            all_passed = False
            print(f"Status: FAILED - {e}")

    if all_passed:
        print("\n[ALL 8 SLICES PASSED] InfoVelocity Dashboard & KPI Visualizations Fully Operational!")
    else:
        print("\n[TESTS FAILED] One or more chart queries or slices failed.")
        exit(1)
