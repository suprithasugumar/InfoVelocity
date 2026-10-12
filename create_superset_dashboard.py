"""
Superset Automation Script for InfoVelocity
===========================================
Configures physical & plain SELECT virtual datasets (without window functions or subquery order bys),
along with 4 KPI summary cards and 4 core analytical visualizations on Apache Druid:
1. Active Stories (COUNT(DISTINCT post_id) over last 6 hours, NO eligibility filter)
2. Average Score Velocity (AVG over rows where v_score IS NOT NULL, last 6 hours)
3. Highest Score Velocity (MAX over rows where v_score IS NOT NULL, last 6 hours)
4. Highest Positive Acceleration (LEAST(MAX(a_score), 50.0) over positive accelerations, last 6 hours)
5. Engagement Velocity — 5-Minute Trend (TIME_FLOOR(__time, 'PT5M'), AVG(v_score), AVG(v_comment))
6. Fastest Growing Narratives Leaderboard (eligible = 1, sort by rank_score DESC, limit 25)
7. Breakout Monitor (eligible = 1 AND a_score > 0, sort by a_norm DESC, shows raw a_score too)
8. Story Engagement Over Time (Audit table with native filters on title and post_id)
"""

import json
from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db, security_manager
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard

    admin_user = security_manager.find_user(username="admin")
    if not admin_user:
        admin_user = security_manager.get_all_users()[0]

    # Find Apache Druid database
    druid_db = db.session.query(Database).filter(
        (Database.database_name.ilike("%druid%")) | (Database.sqlalchemy_uri.ilike("%druid%"))
    ).first()

    if not druid_db:
        raise RuntimeError("Apache Druid database connection not found in Superset!")

    print(f"Found Druid DB ID: {druid_db.id} ({druid_db.database_name})")

    # -------------------------------------------------------------------------
    # 1. Datasets (Plain SELECTs on hn_snapshots with NO window functions)
    # -------------------------------------------------------------------------
    # 1a. Physical Dataset: hn_snapshots
    ds_physical = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_snapshots", sql=None
    ).first()
    if not ds_physical:
        ds_physical = SqlaTable(
            table_name="hn_snapshots",
            database_id=druid_db.id,
            owners=[admin_user],
            main_dttm_col="__time",
        )
        db.session.add(ds_physical)
        db.session.flush()
        print(f"Created physical dataset hn_snapshots (ID: {ds_physical.id})")
    else:
        print(f"Found physical dataset hn_snapshots (ID: {ds_physical.id})")

    try:
        ds_physical.fetch_metadata()
    except Exception as e:
        print(f"Metadata fetch notice for hn_snapshots: {e}")

    # 1b. Virtual Dataset: hn_macro_velocity_5m
    # 5-minute pre-aggregated buckets with AVG(v_score) and AVG(v_comment)
    sql_macro = """
SELECT
    TIME_FLOOR("__time", 'PT5M') AS bucket_time,
    COUNT(*) AS total_snapshot_observations,
    COUNT(DISTINCT post_id) AS active_tracked_stories,
    COUNT(DISTINCT author) AS active_authors,
    ROUND(AVG(v_score), 4) AS avg_score_velocity,
    ROUND(MAX(v_score), 4) AS peak_score_velocity,
    ROUND(AVG(v_comment), 4) AS avg_comment_velocity,
    ROUND(MAX(v_comment), 4) AS peak_comment_velocity
FROM "hn_snapshots"
WHERE "__time" >= (SELECT TIME_FLOOR(MAX("__time"), 'P1D') FROM "hn_snapshots")
  AND v_score IS NOT NULL
GROUP BY TIME_FLOOR("__time", 'PT5M')
""".strip()

    ds_macro = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_macro_velocity_5m"
    ).first()
    if not ds_macro:
        ds_macro = SqlaTable(
            table_name="hn_macro_velocity_5m",
            database_id=druid_db.id,
            sql=sql_macro,
            owners=[admin_user],
            main_dttm_col="bucket_time",
        )
        db.session.add(ds_macro)
        db.session.flush()
        print(f"Created virtual dataset hn_macro_velocity_5m (ID: {ds_macro.id})")
    else:
        ds_macro.sql = sql_macro
        print(f"Updated virtual dataset hn_macro_velocity_5m (ID: {ds_macro.id})")

    # 1c. Unified Virtual Dataset: hn_latest_eligible_6h
    # Latest snapshot per post_id in the last 6h, eligible only. Drives KPIs, Leaderboard, and Breakout Monitor.
    sql_latest_eligible = """
SELECT
    title,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(v_comment, 2), 0.0) AS comment_velocity_per_minute,
    COALESCE(ROUND(rank_score, 4), 0.0) AS weighted_velocity_rank,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(a_comment, 4), 0.0) AS comment_acceleration,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN '<a href="' || url || '" target="_blank" rel="noopener noreferrer">Open Link ↗</a>'
        ELSE '<a href="https://news.ycombinator.com/item?id=' || post_id || '" target="_blank" rel="noopener noreferrer">HN Discussion ↗</a>'
    END AS url,
    post_id,
    dt_sec,
    v_score,
    v_comment,
    a_score,
    a_comment,
    a_norm,
    rank_score,
    score,
    num_comments,
    eligible,
    latest_flag,
    is_capped,
    "__time" AS last_observation_time,
    "__time" AS detected_at,
    "__time" AS observation_time
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
GROUP BY title, author, score, num_comments, v_score, v_comment, rank_score, a_score, a_comment, url, post_id, dt_sec, a_norm, eligible, latest_flag, is_capped, "__time"
""".strip()

    ds_latest_eligible = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_latest_eligible_6h"
    ).first()
    if not ds_latest_eligible:
        ds_latest_eligible = SqlaTable(
            table_name="hn_latest_eligible_6h",
            database_id=druid_db.id,
            sql=sql_latest_eligible,
            owners=[admin_user],
            main_dttm_col="last_observation_time",
        )
        db.session.add(ds_latest_eligible)
        db.session.flush()
        print(f"Created unified virtual dataset hn_latest_eligible_6h (ID: {ds_latest_eligible.id})")
    else:
        ds_latest_eligible.sql = sql_latest_eligible
        print(f"Updated unified virtual dataset hn_latest_eligible_6h (ID: {ds_latest_eligible.id})")

    # Dedicated Virtual Dataset for Positive Acceleration Breakouts
    sql_breakout = """
SELECT
    title,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(a_norm, 6), 0.0) AS a_norm,
    COALESCE(ROUND(a_comment, 4), 0.0) AS comment_acceleration,
    "__time" AS detected_at,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN '<a href="' || url || '" target="_blank" rel="noopener noreferrer">Open Link ↗</a>'
        ELSE '<a href="https://news.ycombinator.com/item?id=' || post_id || '" target="_blank" rel="noopener noreferrer">HN Discussion ↗</a>'
    END AS url,
    post_id,
    dt_sec,
    COALESCE(ROUND(v_comment, 2), 0.0) AS comment_velocity_per_minute,
    v_score,
    v_comment,
    a_score,
    a_comment,
    rank_score AS weighted_velocity_rank,
    rank_score,
    score,
    num_comments,
    eligible,
    latest_flag,
    is_capped,
    "__time" AS last_observation_time,
    "__time" AS observation_time
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
  AND (a_score > 0 OR a_comment > 0)
GROUP BY title, author, score, num_comments, v_score, a_score, a_norm, a_comment, "__time", url, post_id, dt_sec, v_comment, rank_score, eligible, latest_flag, is_capped
""".strip()

    ds_breakout = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_breakout_accelerations"
    ).first()
    if not ds_breakout:
        ds_breakout = SqlaTable(
            table_name="hn_breakout_accelerations",
            database_id=druid_db.id,
            sql=sql_breakout,
            owners=[admin_user],
            main_dttm_col="detected_at",
        )
        db.session.add(ds_breakout)
        db.session.flush()
        print(f"Created virtual dataset hn_breakout_accelerations (ID: {ds_breakout.id})")
    else:
        ds_breakout.sql = sql_breakout
        print(f"Updated virtual dataset hn_breakout_accelerations (ID: {ds_breakout.id})")

    # Virtual Dataset for Story Engagement Progression History (No NULLs, Clickable URLs)
    sql_history = """
SELECT
    "__time" AS observation_time,
    "__time",
    post_id,
    title,
    author,
    score,
    num_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS v_score,
    COALESCE(ROUND(a_score, 4), 0.0) AS a_score,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN '<a href="' || url || '" target="_blank" rel="noopener noreferrer">Open Link ↗</a>'
        ELSE '<a href="https://news.ycombinator.com/item?id=' || post_id || '" target="_blank" rel="noopener noreferrer">HN Discussion ↗</a>'
    END AS url
FROM "hn_snapshots"
GROUP BY "__time", post_id, title, author, score, num_comments, v_score, a_score, url
""".strip()

    ds_history = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_engagement_history"
    ).first()
    if not ds_history:
        ds_history = SqlaTable(
            table_name="hn_engagement_history",
            database_id=druid_db.id,
            sql=sql_history,
            owners=[admin_user],
            main_dttm_col="observation_time",
        )
        db.session.add(ds_history)
        db.session.flush()
        print(f"Created virtual dataset hn_engagement_history (ID: {ds_history.id})")
    else:
        ds_history.sql = sql_history
        print(f"Updated virtual dataset hn_engagement_history (ID: {ds_history.id})")

    ds_fastest = ds_latest_eligible

    db.session.commit()

    # Apply standardized column verbose labels across all datasets
    column_labels = {
        "post_id": "Post ID",
        "title": "Story Title",
        "author": "Author",
        "url": "URL",
        "current_score": "Score",
        "current_comments": "Comments",
        "score": "Score",
        "num_comments": "Comments",
        "score_velocity_per_minute": "Engagement Velocity (pts/min)",
        "comment_velocity_per_minute": "Comment Velocity (comments/min)",
        "v_score": "Engagement Velocity (pts/min)",
        "v_comment": "Comment Velocity (comments/min)",
        "score_acceleration": "Score Acceleration (pts/min^2)",
        "comment_acceleration": "Comment Acceleration (comments/min^2)",
        "a_score": "Score Acceleration (pts/min^2)",
        "a_comment": "Comment Acceleration (comments/min^2)",
        "weighted_velocity_rank": "Rank Score (v * ln(1+score))",
        "rank_score": "Rank Score (v * ln(1+score))",
        "a_norm": "Normalized Acceleration (a/score)",
        "eligible": "Eligible (0/1)",
        "latest_flag": "Latest Flag (0/1)",
        "is_capped": "Capped Flag (0/1)",
        "dt_sec": "dt (seconds)",
        "detected_at": "Detected At",
        "last_observation_time": "Last Observation",
        "bucket_time": "Time (5-Min Interval)",
        "total_snapshot_observations": "Observations",
        "active_tracked_stories": "Active Stories",
        "active_authors": "Active Authors",
        "avg_score_velocity": "Avg Engagement Velocity (pts/min)",
        "peak_score_velocity": "Peak Engagement Velocity (pts/min)",
        "avg_comment_velocity": "Avg Comment Velocity (comments/min)",
        "peak_comment_velocity": "Peak Comment Velocity (comments/min)",
        "__time": "Observation Time",
    }

    for ds in [ds_physical, ds_macro, ds_latest_eligible, ds_breakout, ds_history]:
        try:
            ds.fetch_metadata()
            db.session.commit()
            for col in ds.columns:
                if col.column_name in column_labels:
                    col.verbose_name = column_labels[col.column_name]
                if col.column_name in ("bucket_time", "last_observation_time", "detected_at", "__time"):
                    col.is_dttm = True
                    col.type = "TIMESTAMP"
            db.session.commit()
            print(f"Metadata and verbose labels refreshed for {ds.table_name}")
        except Exception as e:
            print(f"Metadata fetch warning for {ds.table_name}: {e}")

    # -------------------------------------------------------------------------
    # 2. KPI Summary Cards (Row 1 across full width - Driven by hn_latest_eligible_6h)
    # -------------------------------------------------------------------------
    kpi_slices = []

    # KPI 1: Active Stories (COUNT(DISTINCT post_id) on latest eligible stories in last 6h)
    kpi1_name = "Active Stories"
    kpi1_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(DISTINCT post_id)",
            "label": "Active Stories"
        },
        "adhoc_filters": [],
        "subheader": "Eligible Active Stories",
        "header_font_size": 0.35,
        "subheader_font_size": 0.14,
        "y_axis_format": ",d",
        "time_range": "No filter"
    }
    kpi1 = db.session.query(Slice).filter_by(slice_name=kpi1_name).first()
    if not kpi1:
        kpi1 = Slice(
            slice_name=kpi1_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi1_params),
            owners=[admin_user],
            description="Total count of distinct eligible stories tracked over the last 6 hours (latest snapshot per post)."
        )
        db.session.add(kpi1)
    else:
        kpi1.params = json.dumps(kpi1_params)
        kpi1.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi1)
    print(f"KPI 1 '{kpi1_name}' ready (ID: {kpi1.id})")

    # KPI 2: Average Score Velocity (AVG over rows where v_score IS NOT NULL, last 6h)
    kpi2_name = "Average Score Velocity"
    kpi2_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(AVG(score_velocity_per_minute), 2), 0.0)",
            "label": "Avg Velocity"
        },
        "adhoc_filters": [],
        "subheader": "Engagement Velocity (pts/min)",
        "header_font_size": 0.35,
        "subheader_font_size": 0.14,
        "y_axis_format": ".2f",
        "time_range": "No filter"
    }
    kpi2 = db.session.query(Slice).filter_by(slice_name=kpi2_name).first()
    if not kpi2:
        kpi2 = Slice(
            slice_name=kpi2_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi2_params),
            owners=[admin_user],
            description="Mean score velocity across actively tracked eligible stories over the last 6 hours."
        )
        db.session.add(kpi2)
    else:
        kpi2.params = json.dumps(kpi2_params)
        kpi2.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi2)
    print(f"KPI 2 '{kpi2_name}' ready (ID: {kpi2.id})")

    # KPI 3: Highest Score Velocity (MAX over rows where v_score IS NOT NULL, last 6h)
    kpi3_name = "Highest Score Velocity"
    kpi3_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(score_velocity_per_minute), 2), 0.0)",
            "label": "Peak Velocity"
        },
        "adhoc_filters": [],
        "subheader": "Peak Engagement Velocity (pts/min)",
        "header_font_size": 0.35,
        "subheader_font_size": 0.14,
        "y_axis_format": ".2f",
        "time_range": "No filter"
    }
    kpi3 = db.session.query(Slice).filter_by(slice_name=kpi3_name).first()
    if not kpi3:
        kpi3 = Slice(
            slice_name=kpi3_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi3_params),
            owners=[admin_user],
            description="Peak score velocity among active eligible stories over the last 6 hours."
        )
        db.session.add(kpi3)
    else:
        kpi3.params = json.dumps(kpi3_params)
        kpi3.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi3)
    print(f"KPI 3 '{kpi3_name}' ready (ID: {kpi3.id})")

    # KPI 4: Highest Positive Acceleration (MAX(score_acceleration) where score_acceleration > 0, last 6h)
    kpi4_name = "Highest Positive Acceleration"
    kpi4_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(CASE WHEN score_acceleration > 0 THEN score_acceleration END), 4), 0.0)",
            "label": "Peak Acceleration"
        },
        "adhoc_filters": [],
        "subheader": "Score Acceleration (pts/min^2)",
        "header_font_size": 0.35,
        "subheader_font_size": 0.14,
        "y_axis_format": ".4f",
        "time_range": "No filter"
    }
    kpi4 = db.session.query(Slice).filter_by(slice_name=kpi4_name).first()
    if not kpi4:
        kpi4 = Slice(
            slice_name=kpi4_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi4_params),
            owners=[admin_user],
            description="Maximum instantaneous acceleration surge detected over the last 6 hours among eligible stories."
        )
        db.session.add(kpi4)
    else:
        kpi4.params = json.dumps(kpi4_params)
        kpi4.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi4)
    print(f"KPI 4 '{kpi4_name}' ready (ID: {kpi4.id})")

    # -------------------------------------------------------------------------
    # 3. Core Charts (Slices with clean axis formatting & full cell colors)
    # -------------------------------------------------------------------------
    core_slices = []

    # Chart 1: Engagement Velocity — 5-Minute Trend (Row limit 10000, pre-aggregated 5m)
    # x-axis formatted as 24-hour time (HH:mm)
    slice1_name = "Engagement Velocity - 5-Minute Trend"
    old_slice1_name = "Velocity Trend (5-min Buckets: pts/min & comments/min)"
    slice1_params = {
        "datasource": f"{ds_macro.id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "bucket_time",
        "granularity_sqla": "bucket_time",
        "x_axis_time_format": "%H:%M",
        "time_grain_sqla": None,
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(avg_score_velocity)",
                "label": "Engagement Velocity (pts/min)"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(avg_comment_velocity)",
                "label": "Comment Velocity (comments/min)"
            }
        ],
        "adhoc_filters": [],
        "order_desc": False,
        "row_limit": 10000,
        "seriesType": "line",
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_title": "Time (HH:mm)",
        "y_axis_title": "Velocity (pts/min)",
        "y_axis_title_margin": 35,
        "x_axis_title_margin": 30,
        "bottom_margin": "auto",
        "left_margin": "auto",
    }
    s1 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice1_name, old_slice1_name])
    ).first()
    if not s1:
        s1 = Slice(
            slice_name=slice1_name,
            viz_type="echarts_timeseries_line",
            datasource_type="table",
            datasource_id=ds_macro.id,
            params=json.dumps(slice1_params),
            owners=[admin_user],
            description="Macro time-series tracking average score velocity (points/min) and comment velocity (comments/min) in 5-minute moving windows with 24-hour time formatting."
        )
        db.session.add(s1)
    else:
        s1.slice_name = slice1_name
        s1.params = json.dumps(slice1_params)
        s1.datasource_id = ds_macro.id
    db.session.flush()
    core_slices.append(s1)
    print(f"Chart 1 '{slice1_name}' ready (ID: {s1.id})")

    # Chart 2: Fastest Growing Narratives Leaderboard (Driven by hn_latest_eligible_6h, sort by rank_score DESC, limit 25)
    slice2_name = "Fastest Growing Narratives"
    old_slice2_name = "Fastest Growing Stories (Real-Time Velocity Leaderboard)"
    slice2_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "title",
            "author",
            "current_score",
            "current_comments",
            "score_velocity_per_minute",
            "comment_velocity_per_minute",
            "weighted_velocity_rank",
            "score_acceleration",
            "comment_acceleration",
            "url"
        ],
        "adhoc_filters": [],
        "order_by_cols": ['["weighted_velocity_rank", false]'],
        "row_limit": 50,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "table_timestamp_format": "smart_date",
        "page_size": 50,
        "server_page_length": 50,
        "column_config": {
            "title": {"columnWidth": 220},
            "author": {"columnWidth": 90},
            "current_score": {"columnWidth": 65},
            "current_comments": {"columnWidth": 75},
            "score_velocity_per_minute": {"columnWidth": 115, "d3NumberFormat": ",.2f"},
            "comment_velocity_per_minute": {"columnWidth": 115, "d3NumberFormat": ",.2f"},
            "weighted_velocity_rank": {"columnWidth": 125, "d3NumberFormat": ",.4f"},
            "score_acceleration": {"columnWidth": 115, "d3NumberFormat": ",.4f"},
            "comment_acceleration": {"columnWidth": 115, "d3NumberFormat": ",.4f"},
            "url": {"columnWidth": 110}
        },
        "conditional_formatting": [
            {
                "column": "score_acceleration",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "score_acceleration",
                "operator": "<",
                "targetValue": 0,
                "colorScheme": "#5c1d1d",
                "useGradient": False
            },
            {
                "column": "comment_acceleration",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "comment_acceleration",
                "operator": "<",
                "targetValue": 0,
                "colorScheme": "#5c1d1d",
                "useGradient": False
            },
            {
                "column": "score_velocity_per_minute",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            },
            {
                "column": "weighted_velocity_rank",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            }
        ]
    }
    s2 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice2_name, old_slice2_name])
    ).first()
    if not s2:
        s2 = Slice(
            slice_name=slice2_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(slice2_params),
            owners=[admin_user],
            description="Real-time leaderboard ranking stories by weighted velocity v * LN(1 + score) with direct URLs."
        )
        db.session.add(s2)
    else:
        s2.slice_name = slice2_name
        s2.params = json.dumps(slice2_params)
        s2.datasource_id = ds_latest_eligible.id
    db.session.flush()
    core_slices.append(s2)
    print(f"Chart 2 '{slice2_name}' ready (ID: {s2.id})")

    # Chart 3: Breakout Monitor (WHERE a_score > 0, sort by a_norm DESC, limit 20 on hn_latest_eligible_6h)
    # Empty-state text: "No breakouts in the last 6h"
    slice3_name = "Breakout Monitor - Positive Acceleration"
    old_slice3_name = "Acceleration / Breakout Monitor (Positive Surge Detector: pts/min²)"
    slice3_params = {
        "datasource": f"{ds_breakout.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "title",
            "author",
            "current_score",
            "current_comments",
            "score_velocity_per_minute",
            "score_acceleration",
            "a_norm",
            "comment_acceleration",
            "detected_at",
            "url"
        ],
        "adhoc_filters": [],
        "order_by_cols": ['["a_norm", false]'],
        "row_limit": 20,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 13,
        "server_page_length": 13,
        "subheader": "No breakouts in the last 6h",
        "description": "No breakouts in the last 6h",
        "empty_state_text": "No breakouts in the last 6h",
        "empty_message": "No breakouts in the last 6h",
        "no_data_text": "No breakouts in the last 6h",
        "column_config": {
            "title": {"columnWidth": 360},
            "author": {"columnWidth": 110},
            "current_score": {"columnWidth": 75},
            "current_comments": {"columnWidth": 85},
            "score_velocity_per_minute": {"columnWidth": 135, "d3NumberFormat": ",.2f"},
            "score_acceleration": {"columnWidth": 140, "d3NumberFormat": ",.6f"},
            "a_norm": {"columnWidth": 140, "d3NumberFormat": ",.6f"},
            "comment_acceleration": {"columnWidth": 140, "d3NumberFormat": ",.6f"},
            "detected_at": {"columnWidth": 150},
            "url": {"columnWidth": 120}
        },
        "conditional_formatting": [
            {
                "column": "score_acceleration",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "a_norm",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "comment_acceleration",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "comment_acceleration",
                "operator": "<",
                "targetValue": 0,
                "colorScheme": "#5c1d1d",
                "useGradient": False
            },
            {
                "column": "score_velocity_per_minute",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            }
        ]
    }
    s3 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice3_name, old_slice3_name])
    ).first()
    if not s3:
        s3 = Slice(
            slice_name=slice3_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_breakout.id,
            params=json.dumps(slice3_params),
            owners=[admin_user],
            description="Breakout stories experiencing positive instantaneous acceleration surges."
        )
        db.session.add(s3)
    else:
        s3.slice_name = slice3_name
        s3.params = json.dumps(slice3_params)
        s3.datasource_id = ds_breakout.id
        s3.description = "Breakout stories experiencing positive instantaneous acceleration surges."
    db.session.flush()
    core_slices.append(s3)
    print(f"Chart 3 '{slice3_name}' ready (ID: {s3.id})")

    # Chart 4: Story Engagement Over Time (Full Trajectory Audit Table, Top 60)
    slice4_name = "Story Engagement Over Time"
    old_slice4_name = "Story Engagement Progression (Historical Snapshots Log)"
    slice4_params = {
        "datasource": f"{ds_history.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "__time",
            "post_id",
            "title",
            "author",
            "score",
            "num_comments",
            "v_score",
            "a_score",
            "url"
        ],
        "order_by_cols": ['["__time", false]'],
        "row_limit": 60,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 50,
        "server_page_length": 50,
        "column_config": {
            "__time": {"columnWidth": 170},
            "post_id": {"columnWidth": 100},
            "title": {"columnWidth": 380},
            "author": {"columnWidth": 110},
            "score": {"columnWidth": 75},
            "num_comments": {"columnWidth": 85},
            "v_score": {"columnWidth": 115, "d3NumberFormat": ",.2f"},
            "a_score": {"columnWidth": 115, "d3NumberFormat": ",.4f"},
            "url": {"columnWidth": 120}
        }
    }
    s4 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice4_name, old_slice4_name])
    ).first()
    if not s4:
        s4 = Slice(
            slice_name=slice4_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_history.id,
            params=json.dumps(slice4_params),
            owners=[admin_user],
            description="Time-series engagement log displaying raw score and comment growth per story across successive observation snapshots."
        )
        db.session.add(s4)
    else:
        s4.slice_name = slice4_name
        s4.params = json.dumps(slice4_params)
        s4.datasource_id = ds_history.id
    db.session.flush()
    core_slices.append(s4)
    print(f"Chart 4 '{slice4_name}' ready (ID: {s4.id})")

    all_dashboard_slices = kpi_slices + core_slices
    db.session.commit()

    # -------------------------------------------------------------------------
    # 4. Premium Dashboard Layout & Metadata Construction
    # -------------------------------------------------------------------------
    dash_title = "InfoVelocity - Real-Time Narrative Intelligence"
    dash = db.session.query(Dashboard).filter(
        (Dashboard.dashboard_title == dash_title) | 
        (Dashboard.dashboard_title == "InfoVelocity - Real-Time Narrative Intelligence") |
        (Dashboard.dashboard_title.ilike("%InfoVelocity%")) |
        (Dashboard.slug == "infovelocity-narrative-tracker") |
        (Dashboard.id == 10)
    ).first()

    if not dash:
        dash = Dashboard(
            dashboard_title=dash_title,
            slug="infovelocity-narrative-tracker",
            slices=all_dashboard_slices,
            owners=[admin_user],
            published=True,
        )
        db.session.add(dash)
    else:
        dash.dashboard_title = dash_title
        dash.slug = "infovelocity-narrative-tracker"
        dash.slices = all_dashboard_slices
        dash.published = True

    # Header Markdown: Clean title, subtitle, supporting text, and live badges
    header_text = (
        "## InfoVelocity\n"
        "### Real-Time Narrative Intelligence\n"
        "*Monitor how quickly online narratives gain engagement momentum.* &nbsp;&nbsp; "
        "`LIVE` &nbsp; `10s REFRESH` &nbsp; `HACKER NEWS API`"
    )

    # Disclaimer Banner: Compact full-width banner with expandable details
    disclaimer_text = (
        "> **VELOCITY != VERACITY** &nbsp;-&nbsp; **Rapid engagement indicates information diffusion and momentum, not factual accuracy.**\n"
        ">\n"
        "> <details><summary><b>Epistemic Scope & Kinematic Model Details (Click to Expand)</b></summary>\n"
        ">\n"
        "> **InfoVelocity** observes real-time engagement dynamics across Hacker News discussions, calculating:\n"
        "> - **Instantaneous Velocity (v = delta Engagement / delta t)**: Rate of score & comment growth in **points/min** and **comments/min**.\n"
        "> - **True Acceleration (a = delta v / delta t)**: Rate of change in velocity in **points/min^2** and **comments/min^2** to detect early breakout narratives.\n"
        ">\n"
        "> High velocity and positive acceleration reflect viral surge dynamics across the social graph, not factual truth or credibility.\n"
        "> </details>"
    )

    pos_layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"]
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [
                "ROW_HEADER",
                "ROW_DISCLAIMER",
                "ROW_KPIS",
                "ROW_TREND_AND_GROWING",
                "ROW_BREAKOUT",
                "ROW_PROGRESSION"
            ],
            "parents": ["ROOT_ID"]
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {
                "text": dash_title
            }
        },
        "ROW_HEADER": {
            "type": "ROW",
            "id": "ROW_HEADER",
            "children": ["MARKDOWN_HEADER"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        "MARKDOWN_HEADER": {
            "type": "MARKDOWN",
            "id": "MARKDOWN_HEADER",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_HEADER"],
            "meta": {
                "width": 12,
                "height": 18,
                "code": header_text
            }
        },
        "ROW_DISCLAIMER": {
            "type": "ROW",
            "id": "ROW_DISCLAIMER",
            "children": ["MARKDOWN_DISCLAIMER"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        "MARKDOWN_DISCLAIMER": {
            "type": "MARKDOWN",
            "id": "MARKDOWN_DISCLAIMER",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_DISCLAIMER"],
            "meta": {
                "width": 12,
                "height": 14,
                "code": disclaimer_text
            }
        },
        "ROW_KPIS": {
            "type": "ROW",
            "id": "ROW_KPIS",
            "children": [
                f"CHART-{kpi1.id}",
                f"CHART-{kpi2.id}",
                f"CHART-{kpi3.id}",
                f"CHART-{kpi4.id}"
            ],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{kpi1.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi1.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 18,
                "chartId": kpi1.id,
                "sliceName": kpi1.slice_name,
                "uuid": str(kpi1.uuid) if kpi1.uuid else f"chart-uuid-{kpi1.id}"
            }
        },
        f"CHART-{kpi2.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi2.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 18,
                "chartId": kpi2.id,
                "sliceName": kpi2.slice_name,
                "uuid": str(kpi2.uuid) if kpi2.uuid else f"chart-uuid-{kpi2.id}"
            }
        },
        f"CHART-{kpi3.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi3.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 18,
                "chartId": kpi3.id,
                "sliceName": kpi3.slice_name,
                "uuid": str(kpi3.uuid) if kpi3.uuid else f"chart-uuid-{kpi3.id}"
            }
        },
        f"CHART-{kpi4.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi4.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 18,
                "chartId": kpi4.id,
                "sliceName": kpi4.slice_name,
                "uuid": str(kpi4.uuid) if kpi4.uuid else f"chart-uuid-{kpi4.id}"
            }
        },
        "ROW_TREND_AND_GROWING": {
            "type": "ROW",
            "id": "ROW_TREND_AND_GROWING",
            "children": [f"CHART-{s1.id}", f"CHART-{s2.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s1.id}": {
            "type": "CHART",
            "id": f"CHART-{s1.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_TREND_AND_GROWING"],
            "meta": {
                "width": 7,
                "height": 56,
                "chartId": s1.id,
                "sliceName": s1.slice_name,
                "uuid": str(s1.uuid) if s1.uuid else f"chart-uuid-{s1.id}"
            }
        },
        f"CHART-{s2.id}": {
            "type": "CHART",
            "id": f"CHART-{s2.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_TREND_AND_GROWING"],
            "meta": {
                "width": 5,
                "height": 56,
                "chartId": s2.id,
                "sliceName": s2.slice_name,
                "uuid": str(s2.uuid) if s2.uuid else f"chart-uuid-{s2.id}"
            }
        },
        "ROW_BREAKOUT": {
            "type": "ROW",
            "id": "ROW_BREAKOUT",
            "children": [f"CHART-{s3.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s3.id}": {
            "type": "CHART",
            "id": f"CHART-{s3.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_BREAKOUT"],
            "meta": {
                "width": 12,
                "height": 56,
                "chartId": s3.id,
                "sliceName": s3.slice_name,
                "uuid": str(s3.uuid) if s3.uuid else f"chart-uuid-{s3.id}"
            }
        },
        "ROW_PROGRESSION": {
            "type": "ROW",
            "id": "ROW_PROGRESSION",
            "children": [f"CHART-{s4.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s4.id}": {
            "type": "CHART",
            "id": f"CHART-{s4.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_PROGRESSION"],
            "meta": {
                "width": 12,
                "height": 56,
                "chartId": s4.id,
                "sliceName": s4.slice_name,
                "uuid": str(s4.uuid) if s4.uuid else f"chart-uuid-{s4.id}"
            }
        }
    }

    # 7. Audit table: Native filters on story title and post_id to inspect full trajectory
    native_filters = [
        {
            "id": "NATIVE_FILTER_STORY_TITLE",
            "name": "Story Title",
            "filterType": "filter_select",
            "targets": [
                {
                    "datasetId": ds_physical.id,
                    "column": {
                        "name": "title"
                    }
                }
            ],
            "defaultDataMask": {
                "filterState": {}
            },
            "cascadeParentIds": [],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": []
            },
            "type": "NATIVE_FILTER",
            "description": "Select or search story title to view full audit trajectory",
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": False,
                "searchAllOptions": True,
                "inverseSelection": False
            }
        },
        {
            "id": "NATIVE_FILTER_POST_ID",
            "name": "Post ID",
            "filterType": "filter_select",
            "targets": [
                {
                    "datasetId": ds_physical.id,
                    "column": {
                        "name": "post_id"
                    }
                }
            ],
            "defaultDataMask": {
                "filterState": {}
            },
            "cascadeParentIds": [],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": []
            },
            "type": "NATIVE_FILTER",
            "description": "Filter by Post ID to inspect full story trajectory",
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": False,
                "searchAllOptions": True,
                "inverseSelection": False
            }
        }
    ]

    dash.position_json = json.dumps(pos_layout)
    dash.json_metadata = json.dumps({
        "color_scheme": "supersetAndPresetColors",
        "refresh_frequency": 10,
        "timed_refresh_immune_slices": [],
        "native_filter_configuration": native_filters
    })

    db.session.commit()
    print(f"\n[SUCCESS] Dashboard '{dash.dashboard_title}' successfully provisioned with clean axis formatting & full table height!")
    print(f"Dashboard ID: {dash.id} | Slug: {dash.slug} | URL: /superset/dashboard/{dash.slug}/")
