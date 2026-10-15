# -*- coding: utf-8 -*-
"""
Superset Automation Script for InfoVelocity
===========================================
Provisions physical & plain SELECT virtual datasets on Apache Druid,
along with 5 KPI summary cards and 6 analytical visualizations:
1. Active Stories (COUNT(DISTINCT post_id), 30m trendline, single-line title)
2. Average Score Velocity (AVG v_score, 30m trendline, single-line title)
3. Highest Score Velocity (MAX v_score, 30m trendline, single-line title)
4. Current Top Acceleration (MAX a_score from latest snapshots, single-line title)
5. Peak Acceleration (last 6h) (LEAST(MAX(a_score), 50.0), single-line title)
6. Engagement Velocity — 5-Minute Trend (echarts_timeseries_line)
7. Momentum Map (bubble_v2 scatter with SQRT size scaling and opacity)
8. Fastest Growing Narratives Leaderboard (table, row limit 200, no horizontal scroll)
9. Breakout Monitor — Positive Acceleration (table, row limit 100, no horizontal scroll)
10. Narrative Clusters (table, cluster, stories count, combined score, combined velocity)
11. Story Engagement Over Time (Audit table, observation time DESC, row limit 500)
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
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
    # 1. Datasets (Clean SELECTs on hn_snapshots with NO correlated subqueries)
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

    # 1b. Macro 5-Minute Trend Virtual Dataset: hn_macro_velocity_5m
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
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
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
    # Uses Python-precomputed display dimensions (title_link, story_age, observation_time_ist, quadrant, clusters)
    sql_latest_eligible = """
SELECT
    post_id,
    title,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(v_comment, 2), 0.0) AS comment_velocity_per_minute,
    COALESCE(ROUND(rank_score, 4), 0.0) AS weighted_velocity_rank,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(a_comment, 4), 0.0) AS comment_acceleration,
    COALESCE(ROUND(a_norm, 6), 0.0) AS a_norm,
    COALESCE(quadrant, 'Fading / Dormant') AS quadrant,
    COALESCE(story_age, '0m') AS story_age,
    COALESCE(observation_time_ist, '') AS observation_time_ist,
    COALESCE(title_link, title) AS title_link,
    cluster_id,
    cluster_label,
    dt_sec,
    v_score,
    v_comment,
    a_score,
    a_comment,
    rank_score,
    score,
    num_comments,
    eligible,
    latest_flag,
    is_capped,
    "__time" AS last_observation_time
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, rank_score, a_score, a_comment, a_norm, quadrant, story_age, observation_time_ist, title_link, cluster_id, cluster_label, created_at, url, dt_sec, eligible, latest_flag, is_capped, "__time"
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

    # 1d. Dedicated Virtual Dataset for Positive Acceleration Breakouts
    sql_breakout = """
SELECT
    post_id,
    title,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(a_norm, 6), 0.0) AS a_norm,
    COALESCE(ROUND(a_comment, 4), 0.0) AS comment_acceleration,
    COALESCE(story_age, '0m') AS story_age,
    COALESCE(observation_time_ist, '') AS observation_time_ist,
    COALESCE(title_link, title) AS title_link,
    cluster_id,
    cluster_label,
    dt_sec,
    COALESCE(ROUND(v_comment, 2), 0.0) AS comment_velocity_per_minute,
    v_score,
    v_comment,
    a_comment,
    score,
    num_comments,
    eligible,
    latest_flag,
    "__time" AS detected_at
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
  AND a_score > 0
GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, a_score, a_norm, a_comment, story_age, observation_time_ist, title_link, cluster_id, cluster_label, created_at, url, dt_sec, eligible, latest_flag, "__time"
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

    # 1e. Historical Engagement Trajectory Dataset (Audit Table)
    # Fully precomputed display columns; pure GROUP BY and plain __time DESC sorting!
    sql_history = """
SELECT
    post_id,
    title,
    author,
    score,
    num_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS v_score,
    COALESCE(ROUND(a_score, 4), 0.0) AS a_score,
    COALESCE(story_age, '0m') AS story_age,
    COALESCE(observation_time_ist, '') AS observation_time_ist,
    COALESCE(title_link, title) AS title_link,
    cluster_id,
    cluster_label,
    dt_sec,
    eligible,
    "__time"
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
GROUP BY post_id, title, author, score, num_comments, v_score, a_score, story_age, observation_time_ist, title_link, cluster_id, cluster_label, created_at, url, dt_sec, eligible, "__time"
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
            main_dttm_col="__time",
        )
        db.session.add(ds_history)
        db.session.flush()
        print(f"Created virtual dataset hn_engagement_history (ID: {ds_history.id})")
    else:
        ds_history.sql = sql_history
        print(f"Updated virtual dataset hn_engagement_history (ID: {ds_history.id})")

    # 1f. Narrative Clusters Dataset (New Feature)
    sql_clusters = """
SELECT
    cluster_label AS cluster,
    cluster_id,
    COUNT(DISTINCT post_id) AS number_of_stories,
    SUM(score) AS combined_score,
    ROUND(SUM(COALESCE(v_score, 0.0)), 2) AS combined_velocity,
    MAX("__time") AS latest_activity_time
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND cluster_label IS NOT NULL
GROUP BY cluster_label, cluster_id
""".strip()

    ds_clusters = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_narrative_clusters"
    ).first()
    if not ds_clusters:
        ds_clusters = SqlaTable(
            table_name="hn_narrative_clusters",
            database_id=druid_db.id,
            sql=sql_clusters,
            owners=[admin_user],
            main_dttm_col="latest_activity_time",
        )
        db.session.add(ds_clusters)
        db.session.flush()
        print(f"Created virtual dataset hn_narrative_clusters (ID: {ds_clusters.id})")
    else:
        ds_clusters.sql = sql_clusters
        print(f"Updated virtual dataset hn_narrative_clusters (ID: {ds_clusters.id})")

    db.session.commit()

    # Apply standardized column verbose labels across all datasets
    column_labels = {
        "title_link": "Story Title",
        "post_id": "Post ID",
        "title": "Story Title (Raw)",
        "author": "Author",
        "current_score": "Score",
        "current_comments": "Comments",
        "score": "Score",
        "num_comments": "Comments",
        "score_velocity_per_minute": "Velocity (pts/min)",
        "comment_velocity_per_minute": "Comment Velocity (comm/min)",
        "v_score": "Velocity (pts/min)",
        "v_comment": "Comment Velocity (comm/min)",
        "score_acceleration": "Acceleration (pts/min²)",
        "comment_acceleration": "Comment Accel (comm/min²)",
        "a_score": "Acceleration (pts/min²)",
        "a_comment": "Comment Accel (comm/min²)",
        "a_norm": "Normalized Accel (a/score)",
        "weighted_velocity_rank": "Weighted Rank",
        "quadrant": "Momentum Quadrant",
        "story_age": "Story Age",
        "observation_time_ist": "Time (IST)",
        "cluster": "Narrative Cluster",
        "cluster_label": "Narrative Cluster",
        "cluster_id": "Cluster ID",
        "number_of_stories": "Stories",
        "combined_score": "Total Score",
        "combined_velocity": "Total Velocity (pts/min)",
        "bucket_time": "Observation Window",
        "avg_score_velocity": "Avg Score Velocity",
        "avg_comment_velocity": "Avg Comment Velocity"
    }

    for ds in [ds_physical, ds_macro, ds_latest_eligible, ds_breakout, ds_history, ds_clusters]:
        try:
            ds.fetch_metadata()
            for col in ds.columns:
                if col.column_name in column_labels:
                    col.verbose_name = column_labels[col.column_name]
                if col.column_name in ("bucket_time", "last_observation_time", "detected_at", "latest_activity_time", "__time"):
                    col.is_dttm = True
                    col.type = "TIMESTAMP"
            db.session.commit()
            print(f"Metadata refreshed for {ds.table_name}")
        except Exception as e:
            print(f"Metadata warning for {ds.table_name}: {e}")

    # -------------------------------------------------------------------------
    # 2. KPI Summary Cards (Single-line titles, ~48px value font, readable subtitles, 30m delta)
    # -------------------------------------------------------------------------
    kpi_slices = []

    # KPI 1: Active Stories
    kpi1_name = "Active Stories"
    kpi1_params = {
        "datasource": f"{ds_physical.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(DISTINCT post_id)",
            "label": "Active Stories"
        },
        "adhoc_filters": [],
        "granularity_sqla": "__time",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Active distinct threads (last 6h)",
        "header_font_size": 0.5,
        "subheader_font_size": 0.15,
        "y_axis_format": ",d",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi1 = db.session.query(Slice).filter_by(slice_name=kpi1_name).first()
    if not kpi1:
        kpi1 = Slice(
            slice_name=kpi1_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_physical.id,
            params=json.dumps(kpi1_params),
            owners=[admin_user],
            description="Active distinct stories tracked in the last 6 hours with 30-min comparison."
        )
        db.session.add(kpi1)
    else:
        kpi1.params = json.dumps(kpi1_params)
        kpi1.datasource_id = ds_physical.id
    db.session.flush()
    kpi_slices.append(kpi1)
    print(f"KPI 1 '{kpi1_name}' ready (ID: {kpi1.id})")

    # KPI 2: Avg Velocity
    kpi2_name = "Avg Velocity"
    old_kpi2_names = ["Average Score Velocity"]
    kpi2_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(AVG(score_velocity_per_minute), 2), 0.0)",
            "label": "Avg Velocity"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_velocity_per_minute IS NOT NULL",
                "filterOptionName": "filter_v_not_null"
            }
        ],
        "granularity_sqla": "last_observation_time",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Mean score velocity (pts/min)",
        "header_font_size": 0.5,
        "subheader_font_size": 0.15,
        "y_axis_format": ".2f",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi2 = db.session.query(Slice).filter(
        Slice.slice_name.in_([kpi2_name] + old_kpi2_names)
    ).first()
    if not kpi2:
        kpi2 = Slice(
            slice_name=kpi2_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi2_params),
            owners=[admin_user],
            description="Average score velocity across active eligible stories."
        )
        db.session.add(kpi2)
    else:
        kpi2.slice_name = kpi2_name
        kpi2.params = json.dumps(kpi2_params)
        kpi2.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi2)
    print(f"KPI 2 '{kpi2_name}' ready (ID: {kpi2.id})")

    # KPI 3: Peak Velocity
    kpi3_name = "Peak Velocity"
    old_kpi3_names = ["Highest Score Velocity"]
    kpi3_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(score_velocity_per_minute), 2), 0.0)",
            "label": "Peak Velocity"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_velocity_per_minute IS NOT NULL",
                "filterOptionName": "filter_peak_v_not_null"
            }
        ],
        "granularity_sqla": "last_observation_time",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Fastest gaining story (pts/min)",
        "header_font_size": 0.5,
        "subheader_font_size": 0.15,
        "y_axis_format": ".2f",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi3 = db.session.query(Slice).filter(
        Slice.slice_name.in_([kpi3_name] + old_kpi3_names)
    ).first()
    if not kpi3:
        kpi3 = Slice(
            slice_name=kpi3_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(kpi3_params),
            owners=[admin_user],
            description="Peak score velocity among active stories."
        )
        db.session.add(kpi3)
    else:
        kpi3.slice_name = kpi3_name
        kpi3.params = json.dumps(kpi3_params)
        kpi3.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi3)
    print(f"KPI 3 '{kpi3_name}' ready (ID: {kpi3.id})")

    # KPI 4: Current Top Accel
    kpi4_name = "Current Top Accel"
    old_kpi4_names = ["Current Top Acceleration"]
    kpi4_params = {
        "datasource": f"{ds_breakout.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(score_acceleration), 4), 0.0)",
            "label": "Top Surge Accel"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_acceleration > 0",
                "filterOptionName": "filter_curr_accel_pos"
            }
        ],
        "granularity_sqla": "detected_at",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Latest breakout surge (pts/min²)",
        "header_font_size": 0.5,
        "subheader_font_size": 0.15,
        "y_axis_format": ".4f",
        "color_picker": {"r": 52, "g": 211, "b": 153, "a": 1}
    }
    kpi4 = db.session.query(Slice).filter(
        Slice.slice_name.in_([kpi4_name] + old_kpi4_names)
    ).first()
    if not kpi4:
        kpi4 = Slice(
            slice_name=kpi4_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_breakout.id,
            params=json.dumps(kpi4_params),
            owners=[admin_user],
            description="Instantaneous top positive acceleration among latest snapshots."
        )
        db.session.add(kpi4)
    else:
        kpi4.slice_name = kpi4_name
        kpi4.params = json.dumps(kpi4_params)
        kpi4.datasource_id = ds_breakout.id
    db.session.flush()
    kpi_slices.append(kpi4)
    print(f"KPI 4 '{kpi4_name}' ready (ID: {kpi4.id})")

    # KPI 5: Peak Accel (6h)
    kpi5_name = "Peak Accel (6h)"
    old_kpi5_names = ["Peak Acceleration (last 6h)", "Highest Positive Acceleration", "Peak Acceleration"]
    kpi5_params = {
        "datasource": f"{ds_physical.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(LEAST(MAX(CASE WHEN a_score > 0 THEN a_score END), 50.0), 4), 0.0)",
            "label": "Peak Accel (6h)"
        },
        "adhoc_filters": [],
        "granularity_sqla": "__time",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Max 6h surge rate (Cap 50 pts/min²)",
        "header_font_size": 0.5,
        "subheader_font_size": 0.15,
        "y_axis_format": ".4f",
        "color_picker": {"r": 52, "g": 211, "b": 153, "a": 1}
    }
    kpi5 = db.session.query(Slice).filter(
        Slice.slice_name.in_([kpi5_name] + old_kpi5_names)
    ).first()
    if not kpi5:
        kpi5 = Slice(
            slice_name=kpi5_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_physical.id,
            params=json.dumps(kpi5_params),
            owners=[admin_user],
            description="Maximum instantaneous acceleration surge detected over the last 6 hours (sane cap 50.0)."
        )
        db.session.add(kpi5)
    else:
        kpi5.slice_name = kpi5_name
        kpi5.params = json.dumps(kpi5_params)
        kpi5.datasource_id = ds_physical.id
    db.session.flush()
    kpi_slices.append(kpi5)
    print(f"KPI 5 '{kpi5_name}' ready (ID: {kpi5.id})")

    # -------------------------------------------------------------------------
    # 3. Core Analytical Visualizations (6 Charts)
    # -------------------------------------------------------------------------
    core_slices = []

    # Chart 1: Engagement Velocity — 5-Minute Trend (echarts_timeseries_line)
    slice1_name = "Engagement Velocity — 5-Minute Trend"
    old_slice1_names = ["Velocity Trend (5-min Buckets: pts/min & comments/min)", "Engagement Velocity ??? 5-Minute Trend"]
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
        "x_axis_title": "Time (UTC)",
        "y_axis_title": "Velocity (pts/min)",
        "y_axis_title_margin": 35,
        "x_axis_title_margin": 30,
        "bottom_margin": "auto",
        "left_margin": "auto",
        "color_scheme": "supersetColors"
    }
    s1 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice1_name] + old_slice1_names)
    ).first()
    if not s1:
        s1 = Slice(
            slice_name=slice1_name,
            viz_type="echarts_timeseries_line",
            datasource_type="table",
            datasource_id=ds_macro.id,
            params=json.dumps(slice1_params),
            owners=[admin_user],
            description="Macro time-series tracking average score velocity (points/min) and comment velocity (comments/min) in 5-minute moving windows."
        )
        db.session.add(s1)
    else:
        s1.slice_name = slice1_name
        s1.params = json.dumps(slice1_params)
        s1.datasource_id = ds_macro.id
    db.session.flush()
    core_slices.append(s1)
    print(f"Chart 1 '{slice1_name}' ready (ID: {s1.id})")

    # Chart 2: Momentum Map (bubble_v2 scatter with sqrt size scaling and opacity)
    slice2_name = "Momentum Map"
    slice2_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "bubble_v2",
        "entity": "title",
        "series": "quadrant",
        "x": {
            "expressionType": "SQL",
            "sqlExpression": "AVG(score_velocity_per_minute)",
            "label": "Score Velocity (pts/min)"
        },
        "y": {
            "expressionType": "SQL",
            "sqlExpression": "AVG(score_acceleration)",
            "label": "Score Acceleration (pts/min²)"
        },
        "size": {
            "expressionType": "SQL",
            "sqlExpression": "ROUND(SQRT(GREATEST(MAX(current_score), 1)), 2)",
            "label": "Sqrt(Score)"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_velocity_per_minute IS NOT NULL AND score_acceleration IS NOT NULL",
                "filterOptionName": "filter_momentum_valid"
            }
        ],
        "min_bubble_size": "8",
        "max_bubble_size": "32",
        "opacity": 0.65,
        "show_legend": True,
        "legendOrientation": "top",
        "truncateXAxis": False,
        "row_limit": 200,
        "order_desc": True,
        "x_axis_label": "Velocity (pts/min)",
        "y_axis_label": "Acceleration (pts/min²)",
        "color_scheme": "supersetColors"
    }
    s2 = db.session.query(Slice).filter_by(slice_name=slice2_name).first()
    if not s2:
        s2 = Slice(
            slice_name=slice2_name,
            viz_type="bubble_v2",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(slice2_params),
            owners=[admin_user],
            description="Momentum scatter plot mapping discussions across 4 kinematics quadrants."
        )
        db.session.add(s2)
    else:
        s2.params = json.dumps(slice2_params)
        s2.datasource_id = ds_latest_eligible.id
    db.session.flush()
    core_slices.append(s2)
    print(f"Chart 2 '{slice2_name}' ready (ID: {s2.id})")

    # Chart 3: Fastest Growing Narratives Leaderboard (table, row limit 200, no horizontal scroll)
    slice3_name = "Fastest Growing Narratives"
    old_slice3_names = ["Fastest Growing Stories (Real-Time Velocity Leaderboard)"]
    slice3_params = {
        "datasource": f"{ds_latest_eligible.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "title_link",
            "author",
            "current_score",
            "current_comments",
            "score_velocity_per_minute",
            "comment_velocity_per_minute",
            "score_acceleration",
            "weighted_velocity_rank",
            "story_age",
            "observation_time_ist"
        ],
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "eligible = 1",
                "filterOptionName": "filter_leaderboard_eligible"
            }
        ],
        "order_by_cols": ['["weighted_velocity_rank", false]'],
        "row_limit": 200,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 25,
        "server_page_length": 25,
        "column_config": {
            "title_link": {"columnWidth": 260},
            "author": {"columnWidth": 75},
            "current_score": {"columnWidth": 55},
            "current_comments": {"columnWidth": 65},
            "score_velocity_per_minute": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "comment_velocity_per_minute": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "score_acceleration": {"columnWidth": 95, "d3NumberFormat": ",.4f"},
            "weighted_velocity_rank": {"columnWidth": 95, "d3NumberFormat": ",.4f"},
            "story_age": {"columnWidth": 60},
            "observation_time_ist": {"columnWidth": 95}
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
    s3 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice3_name] + old_slice3_names)
    ).first()
    if not s3:
        s3 = Slice(
            slice_name=slice3_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_latest_eligible.id,
            params=json.dumps(slice3_params),
            owners=[admin_user],
            description="Real-time leaderboard ranking stories by weighted velocity v * LN(1 + score) with clickable titles."
        )
        db.session.add(s3)
    else:
        s3.slice_name = slice3_name
        s3.params = json.dumps(slice3_params)
        s3.datasource_id = ds_latest_eligible.id
    db.session.flush()
    core_slices.append(s3)
    print(f"Chart 3 '{slice3_name}' ready (ID: {s3.id})")

    # Chart 4: Breakout Monitor — Positive Acceleration (table, row limit 100, no horizontal scroll)
    slice4_name = "Breakout Monitor — Positive Acceleration"
    old_slice4_names = [
        "Acceleration / Breakout Monitor (Positive Surge Detector: pts/min²)",
        "Breakout Monitor - Positive Acceleration",
        "Breakout Monitor ??? Positive Acceleration"
    ]
    slice4_params = {
        "datasource": f"{ds_breakout.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "title_link",
            "author",
            "current_score",
            "current_comments",
            "score_velocity_per_minute",
            "score_acceleration",
            "a_norm",
            "comment_acceleration",
            "story_age",
            "observation_time_ist"
        ],
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_acceleration > 0",
                "filterOptionName": "filter_breakout_pos"
            }
        ],
        "order_by_cols": ['["a_norm", false]'],
        "row_limit": 100,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 25,
        "server_page_length": 25,
        "empty_state_text": "No breakouts in the last 6h",
        "column_config": {
            "title_link": {"columnWidth": 270},
            "author": {"columnWidth": 75},
            "current_score": {"columnWidth": 55},
            "current_comments": {"columnWidth": 65},
            "score_velocity_per_minute": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "score_acceleration": {"columnWidth": 95, "d3NumberFormat": ",.4f"},
            "a_norm": {"columnWidth": 90, "d3NumberFormat": ",.4f"},
            "comment_acceleration": {"columnWidth": 95, "d3NumberFormat": ",.4f"},
            "story_age": {"columnWidth": 60},
            "observation_time_ist": {"columnWidth": 95}
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
                "column": "score_velocity_per_minute",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            }
        ]
    }
    s4 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice4_name] + old_slice4_names)
    ).first()
    if not s4:
        s4 = Slice(
            slice_name=slice4_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_breakout.id,
            params=json.dumps(slice4_params),
            owners=[admin_user],
            description="Breakout stories experiencing positive instantaneous acceleration surges."
        )
        db.session.add(s4)
    else:
        s4.slice_name = slice4_name
        s4.params = json.dumps(slice4_params)
        s4.datasource_id = ds_breakout.id
    db.session.flush()
    core_slices.append(s4)
    print(f"Chart 4 '{slice4_name}' ready (ID: {s4.id})")

    # Chart 5: Narrative Clusters (New Feature: Table showing cluster, stories count, combined score, combined velocity)
    slice_clusters_name = "Narrative Clusters (Last 6h)"
    slice_clusters_params = {
        "datasource": f"{ds_clusters.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "cluster",
            "number_of_stories",
            "combined_score",
            "combined_velocity"
        ],
        "adhoc_filters": [],
        "order_by_cols": ['["combined_velocity", false]'],
        "row_limit": 50,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "include_search": True,
        "page_size": 15,
        "server_page_length": 15,
        "column_config": {
            "cluster": {"columnWidth": 320},
            "number_of_stories": {"columnWidth": 100},
            "combined_score": {"columnWidth": 110},
            "combined_velocity": {"columnWidth": 130, "d3NumberFormat": ",.2f"}
        },
        "conditional_formatting": [
            {
                "column": "combined_velocity",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            }
        ]
    }
    s_clusters = db.session.query(Slice).filter_by(slice_name=slice_clusters_name).first()
    if not s_clusters:
        s_clusters = Slice(
            slice_name=slice_clusters_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_clusters.id,
            params=json.dumps(slice_clusters_params),
            owners=[admin_user],
            description="Narrative cluster aggregation showing story count, combined score, and combined velocity."
        )
        db.session.add(s_clusters)
    else:
        s_clusters.params = json.dumps(slice_clusters_params)
        s_clusters.datasource_id = ds_clusters.id
    db.session.flush()
    core_slices.append(s_clusters)
    print(f"Chart 5 '{slice_clusters_name}' ready (ID: {s_clusters.id})")

    # Chart 6: Story Engagement Over Time (Full Trajectory Audit Table, row limit 500)
    slice5_name = "Story Engagement Over Time"
    old_slice5_names = ["Story Engagement Progression (Historical Snapshots Log)"]
    slice5_params = {
        "datasource": f"{ds_history.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "observation_time_ist",
            "title_link",
            "author",
            "score",
            "num_comments",
            "v_score",
            "a_score",
            "story_age"
        ],
        "order_by_cols": ['["__time", false]'],
        "row_limit": 500,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 50,
        "server_page_length": 50,
        "column_config": {
            "observation_time_ist": {"columnWidth": 95},
            "title_link": {"columnWidth": 420},
            "author": {"columnWidth": 85},
            "score": {"columnWidth": 60},
            "num_comments": {"columnWidth": 65},
            "v_score": {"columnWidth": 90, "d3NumberFormat": ",.2f"},
            "a_score": {"columnWidth": 90, "d3NumberFormat": ",.4f"},
            "story_age": {"columnWidth": 60}
        },
        "conditional_formatting": [
            {
                "column": "a_score",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#1b4d2e",
                "useGradient": False
            },
            {
                "column": "a_score",
                "operator": "<",
                "targetValue": 0,
                "colorScheme": "#5c1d1d",
                "useGradient": False
            },
            {
                "column": "v_score",
                "operator": ">",
                "targetValue": 0,
                "colorScheme": "#162838",
                "useGradient": False
            }
        ]
    }
    s5 = db.session.query(Slice).filter(
        Slice.slice_name.in_([slice5_name] + old_slice5_names)
    ).first()
    if not s5:
        s5 = Slice(
            slice_name=slice5_name,
            viz_type="table",
            datasource_type="table",
            datasource_id=ds_history.id,
            params=json.dumps(slice5_params),
            owners=[admin_user],
            description="Historical engagement log sorted by observation time DESC to trace story trajectories."
        )
        db.session.add(s5)
    else:
        s5.slice_name = slice5_name
        s5.params = json.dumps(slice5_params)
        s5.datasource_id = ds_history.id
    db.session.flush()
    core_slices.append(s5)
    print(f"Chart 6 '{slice5_name}' ready (ID: {s5.id})")

    all_dashboard_slices = kpi_slices + core_slices
    db.session.commit()

    # -------------------------------------------------------------------------
    # 4. Dashboard Layout & Metadata Construction
    # -------------------------------------------------------------------------
    dash_title = "InfoVelocity — Real-Time Narrative Intelligence"
    dash = db.session.query(Dashboard).filter(
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

    # Header Markdown: Title ⚡, Badges (🔴 LIVE, ⏱️ 10s REFRESH, 📡 HACKER NEWS API in order)
    header_text = (
        "# ⚡ InfoVelocity\n"
        "### Real-Time Narrative Intelligence\n"
        "*Monitor how quickly online discussions gain engagement momentum.* &nbsp;&nbsp; "
        "`🔴 LIVE` &nbsp; `⏱️ 10s REFRESH` &nbsp; `📡 HACKER NEWS API`"
    )

    # Disclaimer Banner: Prefixed with ⚠️ ONLY, no scrambled emojis
    disclaimer_text = (
        "> ⚠️ **VELOCITY ≠ VERACITY** &nbsp; **Rapid engagement indicates information diffusion and momentum, not factual accuracy.**\n"
        ">\n"
        "> <details><summary><b>Epistemic Scope & Kinematic Model Details (Click to Expand)</b></summary>\n"
        ">\n"
        "> **InfoVelocity** observes real-time engagement dynamics across Hacker News discussions, calculating:\n"
        "> - **Instantaneous Velocity (v = ΔEngagement / Δt)**: Rate of score & comment growth in **points/min** and **comments/min**.\n"
        "> - **True Acceleration (a = Δv / Δt)**: Rate of change in velocity in **points/min²** and **comments/min²** to detect early breakout narratives.\n"
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
                "ROW_MOMENTUM_AND_TREND",
                "ROW_LEADERBOARD_AND_BREAKOUT",
                "ROW_CLUSTERS",
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
                "height": 16,
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
                "height": 16,
                "code": disclaimer_text
            }
        },
        # Row 1: 5 Reconciled KPIs (2, 2, 2, 3, 3 = 12 columns, equal height 16)
        "ROW_KPIS": {
            "type": "ROW",
            "id": "ROW_KPIS",
            "children": [
                f"CHART-{kpi1.id}",
                f"CHART-{kpi2.id}",
                f"CHART-{kpi3.id}",
                f"CHART-{kpi4.id}",
                f"CHART-{kpi5.id}"
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
                "width": 2,
                "height": 16,
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
                "width": 2,
                "height": 16,
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
                "width": 2,
                "height": 16,
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
                "height": 16,
                "chartId": kpi4.id,
                "sliceName": kpi4.slice_name,
                "uuid": str(kpi4.uuid) if kpi4.uuid else f"chart-uuid-{kpi4.id}"
            }
        },
        f"CHART-{kpi5.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi5.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 16,
                "chartId": kpi5.id,
                "sliceName": kpi5.slice_name,
                "uuid": str(kpi5.uuid) if kpi5.uuid else f"chart-uuid-{kpi5.id}"
            }
        },
        # Row 2: Trend & Momentum Map (6, 6 = 12 columns, height 60)
        "ROW_MOMENTUM_AND_TREND": {
            "type": "ROW",
            "id": "ROW_MOMENTUM_AND_TREND",
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
            "parents": ["ROOT_ID", "GRID_ID", "ROW_MOMENTUM_AND_TREND"],
            "meta": {
                "width": 6,
                "height": 60,
                "chartId": s1.id,
                "sliceName": s1.slice_name,
                "uuid": str(s1.uuid) if s1.uuid else f"chart-uuid-{s1.id}"
            }
        },
        f"CHART-{s2.id}": {
            "type": "CHART",
            "id": f"CHART-{s2.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_MOMENTUM_AND_TREND"],
            "meta": {
                "width": 6,
                "height": 60,
                "chartId": s2.id,
                "sliceName": s2.slice_name,
                "uuid": str(s2.uuid) if s2.uuid else f"chart-uuid-{s2.id}"
            }
        },
        # Row 3: Leaderboard & Breakout Monitor (6, 6 = 12 columns, height 75)
        "ROW_LEADERBOARD_AND_BREAKOUT": {
            "type": "ROW",
            "id": "ROW_LEADERBOARD_AND_BREAKOUT",
            "children": [f"CHART-{s3.id}", f"CHART-{s4.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s3.id}": {
            "type": "CHART",
            "id": f"CHART-{s3.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_LEADERBOARD_AND_BREAKOUT"],
            "meta": {
                "width": 6,
                "height": 75,
                "chartId": s3.id,
                "sliceName": s3.slice_name,
                "uuid": str(s3.uuid) if s3.uuid else f"chart-uuid-{s3.id}"
            }
        },
        f"CHART-{s4.id}": {
            "type": "CHART",
            "id": f"CHART-{s4.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_LEADERBOARD_AND_BREAKOUT"],
            "meta": {
                "width": 6,
                "height": 75,
                "chartId": s4.id,
                "sliceName": s4.slice_name,
                "uuid": str(s4.uuid) if s4.uuid else f"chart-uuid-{s4.id}"
            }
        },
        # Row 4: Narrative Clusters (width 12, height 50)
        "ROW_CLUSTERS": {
            "type": "ROW",
            "id": "ROW_CLUSTERS",
            "children": [f"CHART-{s_clusters.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s_clusters.id}": {
            "type": "CHART",
            "id": f"CHART-{s_clusters.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_CLUSTERS"],
            "meta": {
                "width": 12,
                "height": 50,
                "chartId": s_clusters.id,
                "sliceName": s_clusters.slice_name,
                "uuid": str(s_clusters.uuid) if s_clusters.uuid else f"chart-uuid-{s_clusters.id}"
            }
        },
        # Row 5: Historical Trajectory Audit Log (width 12, height 65)
        "ROW_PROGRESSION": {
            "type": "ROW",
            "id": "ROW_PROGRESSION",
            "children": [f"CHART-{s5.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {
                "background": "BACKGROUND_TRANSPARENT"
            }
        },
        f"CHART-{s5.id}": {
            "type": "CHART",
            "id": f"CHART-{s5.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_PROGRESSION"],
            "meta": {
                "width": 12,
                "height": 65,
                "chartId": s5.id,
                "sliceName": s5.slice_name,
                "uuid": str(s5.uuid) if s5.uuid else f"chart-uuid-{s5.id}"
            }
        }
    }

    # -------------------------------------------------------------------------
    # 5. Native Dashboard Filters
    # -------------------------------------------------------------------------
    native_filters = [
        {
            "id": "NATIVE_FILTER_TIME_RANGE",
            "name": "Time Window",
            "filterType": "filter_time",
            "targets": [
                {"datasetId": ds_physical.id, "column": {"name": "__time"}},
                {"datasetId": ds_macro.id, "column": {"name": "bucket_time"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "last_observation_time"}},
                {"datasetId": ds_breakout.id, "column": {"name": "detected_at"}},
                {"datasetId": ds_history.id, "column": {"name": "__time"}},
                {"datasetId": ds_clusters.id, "column": {"name": "latest_activity_time"}}
            ],
            "defaultDataMask": {
                "filterState": {
                    "value": "6 hours ago : now"
                }
            },
            "cascadeParentIds": [],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": []
            },
            "type": "NATIVE_FILTER",
            "description": "Select observation time window",
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": False,
                "searchAllOptions": False,
                "inverseSelection": False
            }
        },
        {
            "id": "NATIVE_FILTER_MIN_SCORE",
            "name": "Min Score",
            "filterType": "filter_range",
            "targets": [
                {"datasetId": ds_physical.id, "column": {"name": "score"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "current_score"}},
                {"datasetId": ds_breakout.id, "column": {"name": "current_score"}},
                {"datasetId": ds_history.id, "column": {"name": "score"}}
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
            "description": "Filter stories by minimum engagement score threshold",
            "controlValues": {
                "enableEmptyFilter": True
            }
        },
        {
            "id": "NATIVE_FILTER_AUTHOR",
            "name": "Author",
            "filterType": "filter_select",
            "targets": [
                {"datasetId": ds_physical.id, "column": {"name": "author"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "author"}},
                {"datasetId": ds_breakout.id, "column": {"name": "author"}},
                {"datasetId": ds_history.id, "column": {"name": "author"}}
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
            "description": "Filter narratives by author username",
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False
            }
        },
        {
            "id": "NATIVE_FILTER_STORY_TITLE",
            "name": "Story Title",
            "filterType": "filter_select",
            "targets": [
                {"datasetId": ds_physical.id, "column": {"name": "title"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "title"}},
                {"datasetId": ds_breakout.id, "column": {"name": "title"}},
                {"datasetId": ds_history.id, "column": {"name": "title"}}
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
            "description": "Search and filter by story title to trace individual narrative trajectories",
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
                {"datasetId": ds_physical.id, "column": {"name": "post_id"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "post_id"}},
                {"datasetId": ds_breakout.id, "column": {"name": "post_id"}},
                {"datasetId": ds_history.id, "column": {"name": "post_id"}}
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
            "description": "Filter by Hacker News Post ID to view exact snapshot history",
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": False,
                "searchAllOptions": True,
                "inverseSelection": False
            }
        }
    ]

    dash.position_json = json.dumps(pos_layout, ensure_ascii=False)
    dash.json_metadata = json.dumps({
        "color_scheme": "supersetColors",
        "refresh_frequency": 10,
        "timed_refresh_immune_slices": [],
        "native_filter_configuration": native_filters
    }, ensure_ascii=False)

    db.session.commit()
    print(f"\n[SUCCESS] Dashboard '{dash.dashboard_title}' successfully provisioned!")
    print(f"Dashboard ID: {dash.id} | Slug: {dash.slug} | URL: /superset/dashboard/{dash.slug}/")
    print(f"Total Slices Configured: {len(dash.slices)} (5 KPIs + 6 Visualizations)")
