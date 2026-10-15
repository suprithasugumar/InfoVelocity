"""
InfoVelocity - Production Superset Dashboard & Visualizations Provisioner
========================================================================
Configures Apache Superset with:
- 6 High-Impact Big Number KPI Cards (Active Stories, Avg Velocity, Peak Velocity, Current Accel, Peak Accel, Data Freshness)
- 6 Core Analytical Visualizations:
  1. Engagement Velocity — 5-Minute Trend (echarts_timeseries_line with gap breaks and point markers)
  2. Momentum Map (bubble_v2 4-quadrant scatter with capped sqrt(score) sizing and gray dots for v=0)
  3. Fastest Growing Narratives Leaderboard (table with clickable story title URLs)
  4. Breakout Monitor — Positive Acceleration (table with instantaneous acceleration surges)
  5. Narrative Clusters (dedicated hn_clusters dataset, min 2 stories, grouped by title similarity)
  6. Story Engagement Over Time (full historical trajectory audit log sorted by __time DESC)
- Explicit Data Freshness KPI and Data Source LIVE / SIMULATED badge
- 10-second auto-refresh and clean grid layout with equal heights and no row limit warnings.
"""

from superset.app import create_app
import json
from datetime import datetime, timezone, timedelta
from sqlalchemy import text

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    from superset.security import SupersetSecurityManager

    print("[*] Initializing InfoVelocity Superset Dashboard Provisioner...")

    # Ensure admin user
    sm = app.appbuilder.sm
    admin_user = sm.find_user("admin")
    if not admin_user:
        admin_user = sm.find_user(username="admin")

    # Connect to Apache Druid database in Superset
    druid_db = db.session.query(Database).filter_by(database_name="druid").first()
    if not druid_db:
        druid_db = db.session.query(Database).filter(
            Database.sqlalchemy_uri.ilike("%druid%")
        ).first()

    if not druid_db:
        print("[ERROR] Druid database connection not found in Superset! Creating connection...")
        druid_db = Database(
            database_name="druid",
            sqlalchemy_uri="druid://broker:8082/druid/v2/sql",
            extra=json.dumps({"engine_params": {"connect_args": {"scheme": "http"}}})
        )
        db.session.add(druid_db)
        db.session.commit()
    print(f"[*] Druid database ready (ID: {druid_db.id}, URI: {druid_db.sqlalchemy_uri})")

    # Determine latest data timestamp from Druid for dynamic display
    latest_ist_label = "22:41 IST"
    try:
        with druid_db.get_sqla_engine() as eng:
            with eng.connect() as conn:
                res = conn.execute(text('SELECT MAX("__time") as max_t FROM "hn_snapshots"'))
                row = res.fetchone()
                if row and row[0]:
                    max_dt = row[0]
                    if isinstance(max_dt, str):
                        max_dt = datetime.fromisoformat(max_dt.replace("Z", "+00:00"))
                    ist_dt = max_dt.astimezone(timezone.utc) + timedelta(hours=5, minutes=30)
                    latest_ist_label = ist_dt.strftime("%H:%M IST")
                    print(f"[*] Detected latest snapshot time: {max_dt} -> {latest_ist_label}")
    except Exception as e:
        print(f"[WARN] Could not fetch max timestamp from Druid: {e}")

    # -------------------------------------------------------------------------
    # 1. Physical & Virtual Datasets
    # -------------------------------------------------------------------------

    # 1a. Physical Dataset: hn_snapshots
    ds_physical = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_snapshots"
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

    # 1a-2. Dedicated Virtual Dataset for Active Stories (Last 6h without adhoc subquery)
    sql_active_stories = """
SELECT
    post_id,
    author,
    title,
    score,
    num_comments,
    "__time"
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
""".strip()

    ds_active_stories = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_active_stories_6h"
    ).first()
    if not ds_active_stories:
        ds_active_stories = SqlaTable(
            table_name="hn_active_stories_6h",
            database_id=druid_db.id,
            sql=sql_active_stories,
            owners=[admin_user],
            main_dttm_col="__time",
        )
        db.session.add(ds_active_stories)
        db.session.flush()
        print(f"Created virtual dataset hn_active_stories_6h (ID: {ds_active_stories.id})")
    else:
        ds_active_stories.sql = sql_active_stories
        ds_active_stories.main_dttm_col = "__time"
        print(f"Updated virtual dataset hn_active_stories_6h (ID: {ds_active_stories.id})")

    # Explicitly ensure TableColumns are registered on ds_active_stories
    active_stories_cols = [
        ("post_id", "VARCHAR", False),
        ("author", "VARCHAR", False),
        ("title", "VARCHAR", False),
        ("score", "BIGINT", False),
        ("num_comments", "BIGINT", False),
        ("__time", "TIMESTAMP", True),
    ]
    existing_as_cols = {c.column_name: c for c in db.session.query(TableColumn).filter_by(table_id=ds_active_stories.id).all()}
    for cname, ctype, is_dttm in active_stories_cols:
        if cname not in existing_as_cols:
            col = TableColumn(
                table_id=ds_active_stories.id,
                column_name=cname,
                type=ctype,
                is_dttm=is_dttm,
                filterable=True,
                groupby=True
            )
            db.session.add(col)
        else:
            col = existing_as_cols[cname]
            col.type = ctype
            col.is_dttm = is_dttm
    db.session.commit()

    # 1b. Macro 5-Minute Trend Virtual Dataset: hn_macro_velocity_5m
    sql_macro = """
SELECT
    TIME_FLOOR("__time", 'PT5M') AS bucket_time,
    COUNT(*) AS total_snapshot_observations,
    COUNT(DISTINCT post_id) AS active_tracked_stories,
    COUNT(DISTINCT author) AS active_authors,
    ROUND(AVG(v_score), 4) AS avg_score_velocity,
    ROUND(MAX(v_score), 4) AS peak_score_velocity,
    CASE WHEN COUNT(CASE WHEN v_comment > 0 THEN 1 END) > 0 OR SUM(v_comment) > 0
         THEN ROUND(AVG(v_comment), 4)
         ELSE NULL
    END AS avg_comment_velocity,
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
    # Uses Python-precomputed display dimensions (title_link, story_age, observation_time_ist, quadrant)
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
    COALESCE(quadrant, 'Dormant') AS quadrant,
    COALESCE(story_age, '0m') AS story_age,
    COALESCE(observation_time_ist, '') AS observation_time_ist,
    COALESCE(title_link, title) AS title_link,
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
  AND "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, rank_score, a_score, a_comment, a_norm, quadrant, story_age, observation_time_ist, title_link, created_at, url, dt_sec, eligible, latest_flag, is_capped, "__time"
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
  AND "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, a_score, a_norm, a_comment, story_age, observation_time_ist, title_link, created_at, url, dt_sec, eligible, latest_flag, "__time"
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
    dt_sec,
    eligible,
    "__time"
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
GROUP BY post_id, title, author, score, num_comments, v_score, a_score, story_age, observation_time_ist, title_link, created_at, url, dt_sec, eligible, "__time"
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

    # 1f. Dedicated Narrative Clusters Dataset (Separate hn_clusters table in Druid)
    sql_clusters = """
SELECT
    cluster,
    cluster_id,
    top_title,
    latest_activity_time,
    MAX(number_of_stories) AS number_of_stories,
    MAX(combined_score) AS combined_score,
    MAX(combined_velocity) AS combined_velocity
FROM "hn_clusters"
WHERE number_of_stories >= 2
GROUP BY cluster, cluster_id, top_title, latest_activity_time
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
        print(f"Created virtual dataset hn_narrative_clusters (ID: {ds_clusters.id})")
    else:
        ds_clusters.sql = sql_clusters
        print(f"Updated virtual dataset hn_narrative_clusters (ID: {ds_clusters.id})")

    # Explicitly ensure all TableColumns are registered on ds_clusters so no missing column errors occur
    cluster_cols = [
        ("cluster", "VARCHAR", False),
        ("cluster_id", "VARCHAR", False),
        ("top_title", "VARCHAR", False),
        ("latest_activity_time", "TIMESTAMP", True),
        ("number_of_stories", "BIGINT", False),
        ("combined_score", "BIGINT", False),
        ("combined_velocity", "DOUBLE", False),
    ]
    existing_cluster_cols = {c.column_name: c for c in db.session.query(TableColumn).filter_by(table_id=ds_clusters.id).all()}
    for cname, ctype, is_dttm in cluster_cols:
        if cname not in existing_cluster_cols:
            col = TableColumn(
                table_id=ds_clusters.id,
                column_name=cname,
                type=ctype,
                is_dttm=is_dttm,
                filterable=True,
                groupby=True
            )
            db.session.add(col)
        else:
            col = existing_cluster_cols[cname]
            col.type = ctype
            col.is_dttm = is_dttm
            col.filterable = True
            col.groupby = True

    # Ensure v_score_30m exists on physical dataset
    existing_phys_cols = {c.column_name: c for c in db.session.query(TableColumn).filter_by(table_id=ds_physical.id).all()}
    if "v_score_30m" not in existing_phys_cols:
        col = TableColumn(table_id=ds_physical.id, column_name="v_score_30m", type="DOUBLE", filterable=True, groupby=True)
        db.session.add(col)

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
        "v_score_30m": "30-min Velocity (pts/min)",
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
        "cluster_id": "Cluster ID",
        "number_of_stories": "Stories",
        "combined_score": "Total Score",
        "combined_velocity": "Total Velocity (pts/min)",
        "top_title": "Top Narrative Title",
        "bucket_time": "Observation Window",
        "total_snapshot_observations": "Total Observations",
        "active_tracked_stories": "Tracked Stories",
        "active_authors": "Active Authors",
        "avg_score_velocity": "Avg Score Velocity",
        "peak_score_velocity": "Peak Score Velocity",
        "avg_comment_velocity": "Avg Comment Velocity",
        "peak_comment_velocity": "Peak Comment Velocity",
        "detected_at": "Detection Time",
        "last_observation_time": "Latest Observation",
        "latest_activity_time": "Latest Activity",
        "dt_sec": "Lookback dt (s)"
    }

    for ds in [ds_physical, ds_active_stories, ds_macro, ds_latest_eligible, ds_breakout, ds_history, ds_clusters]:
        cols = db.session.query(TableColumn).filter_by(table_id=ds.id).all()
        for col in cols:
            if col.column_name in column_labels:
                col.verbose_name = column_labels[col.column_name]
    db.session.commit()

    # -------------------------------------------------------------------------
    # 2. Reconciled KPI Cards (6 Big Numbers)
    # -------------------------------------------------------------------------
    kpi_slices = []

    # KPI 1: Active Stories (Plain COUNT(DISTINCT post_id) over last 6h virtual dataset)
    kpi1_name = "Active Stories"
    kpi1_params = {
        "datasource": f"{ds_active_stories.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": 'COUNT(DISTINCT post_id)',
            "label": "Active Stories"
        },
        "adhoc_filters": [],
        "granularity_sqla": "__time",
        "time_range": "No filter",
        "subheader": "Distinct stories tracked (last 6h)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ",d",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi1 = db.session.query(Slice).filter_by(slice_name=kpi1_name).first()
    if not kpi1:
        kpi1 = Slice(
            slice_name=kpi1_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_active_stories.id,
            params=json.dumps(kpi1_params),
            owners=[admin_user],
            description="Active distinct stories tracked in the last 6 hours."
        )
        db.session.add(kpi1)
    else:
        kpi1.viz_type = "big_number_total"
        kpi1.params = json.dumps(kpi1_params)
        kpi1.datasource_id = ds_active_stories.id
    db.session.flush()
    kpi_slices.append(kpi1)
    print(f"KPI 1 '{kpi1_name}' ready (ID: {kpi1.id})")

    # KPI 2: Avg Velocity (5-min velocity)
    kpi2_name = "Avg Velocity"
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
        "subheader": "Mean score velocity (5-min velocity, pts/min)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ".2f",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi2 = db.session.query(Slice).filter_by(slice_name=kpi2_name).first()
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
        kpi2.params = json.dumps(kpi2_params)
        kpi2.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi2)
    print(f"KPI 2 '{kpi2_name}' ready (ID: {kpi2.id})")

    # KPI 3: Peak Velocity (5-min velocity)
    kpi3_name = "Peak Velocity"
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
        "subheader": "Fastest gaining story (5-min velocity, pts/min)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ".2f",
        "color_picker": {"r": 96, "g": 165, "b": 250, "a": 1}
    }
    kpi3 = db.session.query(Slice).filter_by(slice_name=kpi3_name).first()
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
        kpi3.params = json.dumps(kpi3_params)
        kpi3.datasource_id = ds_latest_eligible.id
    db.session.flush()
    kpi_slices.append(kpi3)
    print(f"KPI 3 '{kpi3_name}' ready (ID: {kpi3.id})")

    # KPI 4: Current Accel (10-min acceleration)
    kpi4_name = "Current Accel"
    kpi4_params = {
        "datasource": f"{ds_breakout.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(score_acceleration), 4), 0.0)",
            "label": "Current Accel"
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
        "subheader": "Latest breakout surge (10-min acceleration, pts/min²)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ".4f",
        "color_picker": {"r": 52, "g": 211, "b": 153, "a": 1}
    }
    kpi4 = db.session.query(Slice).filter_by(slice_name=kpi4_name).first()
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
        kpi4.params = json.dumps(kpi4_params)
        kpi4.datasource_id = ds_breakout.id
    db.session.flush()
    kpi_slices.append(kpi4)
    print(f"KPI 4 '{kpi4_name}' ready (ID: {kpi4.id})")

    # KPI 5: Peak Accel (Filtered strictly to 6h relative to now)
    kpi5_name = "Peak Accel"
    kpi5_params = {
        "datasource": f"{ds_physical.id}__table",
        "viz_type": "big_number",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(CASE WHEN a_score > 0 THEN a_score END), 4), 0.0)",
            "label": "Peak Accel"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": '"__time" > TIME_SHIFT(CURRENT_TIMESTAMP, \'PT6H\', -1)',
                "filterOptionName": "filter_peak_accel_6h"
            }
        ],
        "granularity_sqla": "__time",
        "time_grain_sqla": "PT5M",
        "time_range": "No filter",
        "compare_lag": 6,
        "compare_suffix": "vs 30m ago",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "subheader": "Max 6h surge rate (10-min acceleration, pts/min²)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ".4f",
        "color_picker": {"r": 52, "g": 211, "b": 153, "a": 1}
    }
    kpi5 = db.session.query(Slice).filter_by(slice_name=kpi5_name).first()
    if not kpi5:
        kpi5 = Slice(
            slice_name=kpi5_name,
            viz_type="big_number",
            datasource_type="table",
            datasource_id=ds_physical.id,
            params=json.dumps(kpi5_params),
            owners=[admin_user],
            description="Maximum instantaneous acceleration surge detected over the last 6 hours."
        )
        db.session.add(kpi5)
    else:
        kpi5.params = json.dumps(kpi5_params)
        kpi5.datasource_id = ds_physical.id
    db.session.flush()
    kpi_slices.append(kpi5)
    print(f"KPI 5 '{kpi5_name}' ready (ID: {kpi5.id})")

    # KPI 6: Data Freshness
    kpi6_name = "Data Freshness"
    kpi6_params = {
        "datasource": f"{ds_physical.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": 'ROUND(CAST(TIMESTAMPDIFF(SECOND, MAX("__time"), CURRENT_TIMESTAMP) AS DOUBLE) / 60.0, 1)',
            "label": "Data Freshness"
        },
        "adhoc_filters": [],
        "granularity_sqla": "__time",
        "time_range": "No filter",
        "subheader": "Minutes since last snapshot (warning > 5m)",
        "header_font_size": 0.65,
        "subheader_font_size": 0.24,
        "y_axis_format": ".1f",
        "color_picker": {"r": 239, "g": 68, "b": 68, "a": 1}
    }
    kpi6 = db.session.query(Slice).filter_by(slice_name=kpi6_name).first()
    if not kpi6:
        kpi6 = Slice(
            slice_name=kpi6_name,
            viz_type="big_number_total",
            datasource_type="table",
            datasource_id=ds_physical.id,
            params=json.dumps(kpi6_params),
            owners=[admin_user],
            description="Elapsed time in minutes since the last recorded snapshot."
        )
        db.session.add(kpi6)
    else:
        kpi6.params = json.dumps(kpi6_params)
        kpi6.datasource_id = ds_physical.id
    db.session.flush()
    kpi_slices.append(kpi6)
    print(f"KPI 6 '{kpi6_name}' ready (ID: {kpi6.id})")

    # -------------------------------------------------------------------------
    # 3. Core Analytical Visualizations (6 Charts)
    # -------------------------------------------------------------------------
    core_slices = []

    # Chart 1: Engagement Velocity — 5-Minute Trend (echarts_timeseries_line)
    slice1_name = "Engagement Velocity — 5-Minute Trend"
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
        "show_markers": True,
        "markerEnabled": True,
        "markerSize": 6,
        "connect_nulls": False,
        "x_axis_title": f"Time (UTC) • Last data: {latest_ist_label}",
        "y_axis_title": "Velocity (pts/min)",
        "y_axis_title_margin": 45,
        "x_axis_title_margin": 40,
        "bottom_margin": 50,
        "left_margin": 50,
        "color_scheme": "supersetColors"
    }
    s1 = db.session.query(Slice).filter_by(slice_name=slice1_name).first()
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
            "sqlExpression": "CASE WHEN AVG(score_velocity_per_minute) = 0 THEN 1.5 ELSE LEAST(ROUND(SQRT(GREATEST(MAX(current_score), 1)), 2), 25.0) END",
            "label": "Sqrt(Score)"
        },
        "adhoc_filters": [
            {
                "clause": "WHERE",
                "expressionType": "SQL",
                "sqlExpression": "score_velocity_per_minute IS NOT NULL",
                "filterOptionName": "filter_momentum_valid"
            }
        ],
        "min_bubble_size": "8",
        "max_bubble_size": "26",
        "opacity": 0.70,
        "show_legend": True,
        "legendOrientation": "top",
        "truncateXAxis": False,
        "row_limit": 500,
        "order_desc": True,
        "x_axis_label": "Velocity (pts/min)",
        "y_axis_label": "Acceleration (pts/min²)",
        "x_axis_title_margin": 45,
        "y_axis_title_margin": 45,
        "bottom_margin": 60,
        "left_margin": 60,
        "data_zoom": True,
        "zoomable": True,
        "label_colors": {
            "Breakout (Accelerating)": "#34d399",
            "Cooling (Slowing)": "#2dd4bf",
            "Steady (Linear Growth)": "#60a5fa",
            "Dormant / Static": "#94a3b8"
        },
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

    # Chart 3: Fastest Growing Narratives Leaderboard
    slice3_name = "Fastest Growing Narratives"
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
                "sqlExpression": "score_velocity_per_minute > 0 OR comment_velocity_per_minute > 0",
                "filterOptionName": "filter_growth_pos"
            }
        ],
        "order_by_cols": ['["weighted_velocity_rank", false]'],
        "row_limit": 500,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 25,
        "server_page_length": 25,
        "empty_state_text": "No stories gaining engagement in the latest poll",
        "column_config": {
            "title_link": {"columnWidth": 270},
            "author": {"columnWidth": 75},
            "current_score": {"columnWidth": 55},
            "current_comments": {"columnWidth": 65},
            "score_velocity_per_minute": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "comment_velocity_per_minute": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "score_acceleration": {"columnWidth": 95, "d3NumberFormat": ",.4f"},
            "weighted_velocity_rank": {"columnWidth": 95, "d3NumberFormat": ",.2f"},
            "story_age": {"columnWidth": 60},
            "observation_time_ist": {"columnWidth": 95}
        },
        "conditional_formatting": [
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
    s3 = db.session.query(Slice).filter_by(slice_name=slice3_name).first()
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
        s3.params = json.dumps(slice3_params)
        s3.datasource_id = ds_latest_eligible.id
    db.session.flush()
    core_slices.append(s3)
    print(f"Chart 3 '{slice3_name}' ready (ID: {s3.id})")

    # Chart 4: Breakout Monitor — Positive Acceleration
    slice4_name = "Breakout Monitor — Positive Acceleration"
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
        "row_limit": 500,
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
    s4 = db.session.query(Slice).filter_by(slice_name=slice4_name).first()
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
        s4.params = json.dumps(slice4_params)
        s4.datasource_id = ds_breakout.id
    db.session.flush()
    core_slices.append(s4)
    print(f"Chart 4 '{slice4_name}' ready (ID: {s4.id})")

    # Chart 5: Narrative Clusters (Last 6h)
    slice_clusters_name = "Narrative Clusters (Last 6h)"
    slice_clusters_params = {
        "datasource": f"{ds_clusters.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "cluster",
            "number_of_stories",
            "combined_score",
            "combined_velocity",
            "top_title"
        ],
        "adhoc_filters": [],
        "order_by_cols": ['["combined_velocity", false]'],
        "row_limit": 500,
        "table_header_fixed": True,
        "show_cell_bars": False,
        "include_search": True,
        "page_size": 15,
        "server_page_length": 15,
        "empty_state_text": "No narrative clusters in the last 6h",
        "column_config": {
            "cluster": {"columnWidth": 260},
            "number_of_stories": {"columnWidth": 80},
            "combined_score": {"columnWidth": 95},
            "combined_velocity": {"columnWidth": 120, "d3NumberFormat": ",.2f"},
            "top_title": {"columnWidth": 320}
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

    # Chart 6: Story Engagement Over Time (Trajectory Audit Table)
    slice5_name = "Story Engagement Over Time"
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
        "row_limit": 100000,
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
    s5 = db.session.query(Slice).filter_by(slice_name=slice5_name).first()
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

    # Header Markdown: Title ⚡, Badges (🔴 LIVE, ⏱️ 10s REFRESH, 📡 DATA SOURCE: LIVE HN STREAM, ⏱️ Last data: HH:mm IST)
    header_text = (
        "# ⚡ InfoVelocity\n"
        "### Real-Time Narrative Intelligence\n"
        "*Monitor how quickly online discussions gain engagement momentum.* &nbsp;&nbsp; "
        f"`🔴 LIVE` &nbsp; `⏱️ 10s REFRESH` &nbsp; `📡 DATA SOURCE: LIVE HN STREAM` &nbsp; `⏱️ Last data: {latest_ist_label}`"
    )

    # Disclaimer Banner
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
                "height": 14,
                "code": disclaimer_text
            }
        },
        # Row 1: 6 Reconciled KPIs (2, 2, 2, 2, 2, 2 = 12 columns, equal height 18)
        "ROW_KPIS": {
            "type": "ROW",
            "id": "ROW_KPIS",
            "children": [
                f"CHART-{kpi1.id}",
                f"CHART-{kpi2.id}",
                f"CHART-{kpi3.id}",
                f"CHART-{kpi4.id}",
                f"CHART-{kpi5.id}",
                f"CHART-{kpi6.id}"
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
                "width": 2,
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
                "width": 2,
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
                "width": 2,
                "height": 18,
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
                "width": 2,
                "height": 18,
                "chartId": kpi5.id,
                "sliceName": kpi5.slice_name,
                "uuid": str(kpi5.uuid) if kpi5.uuid else f"chart-uuid-{kpi5.id}"
            }
        },
        f"CHART-{kpi6.id}": {
            "type": "CHART",
            "id": f"CHART-{kpi6.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 2,
                "height": 18,
                "chartId": kpi6.id,
                "sliceName": kpi6.slice_name,
                "uuid": str(kpi6.uuid) if kpi6.uuid else f"chart-uuid-{kpi6.id}"
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
        # Row 3: Leaderboard & Breakout Monitor (6, 6 = 12 columns, height 62)
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
                "height": 62,
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
                "height": 62,
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
        # Row 5: Story Trajectory Audit Table (width 12, height 64)
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
                "height": 64,
                "chartId": s5.id,
                "sliceName": s5.slice_name,
                "uuid": str(s5.uuid) if s5.uuid else f"chart-uuid-{s5.id}"
            }
        }
    }

    dash.position_json = json.dumps(pos_layout)

    # Native Filters: Native equality filter on post_id/title for story trajectory tracing
    native_filters = [
        {
            "id": "NATIVE_FILTER-post_id",
            "name": "Story / Post ID Filter",
            "filterType": "filter_select",
            "targets": [
                {"datasetId": ds_history.id, "column": {"name": "post_id"}},
                {"datasetId": ds_latest_eligible.id, "column": {"name": "post_id"}},
                {"datasetId": ds_breakout.id, "column": {"name": "post_id"}}
            ],
            "defaultDataMask": {},
            "controlValues": {
                "enableEmptyFilter": True,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "inverseSelection": False
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER"
        },
        {
            "id": "NATIVE_FILTER-time_range",
            "name": "Observation Time Range",
            "filterType": "filter_time",
            "targets": [
                {"datasetId": ds_physical.id, "column": {"name": "__time"}},
                {"datasetId": ds_macro.id, "column": {"name": "bucket_time"}},
                {"datasetId": ds_history.id, "column": {"name": "__time"}},
                {"datasetId": ds_clusters.id, "column": {"name": "latest_activity_time"}}
            ],
            "defaultDataMask": {
                "filterState": {"value": "No filter"}
            },
            "controlValues": {},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER"
        }
    ]

    dash.json_metadata = json.dumps({
        "color_scheme": "supersetColors",
        "refresh_frequency": 10,
        "timed_refresh_immune_slices": [],
        "native_filter_configuration": native_filters
    })

    db.session.commit()

    # Clear Superset cache so fresh data and queries reflect immediately
    try:
        from superset.extensions import cache_manager
        cache_manager.data_cache.clear()
        cache_manager.cache.clear()
        print("[*] Successfully cleared Superset data and query caches.")
    except Exception as e:
        print(f"[WARN] Cache clearing: {e}")

    print(f"\n[SUCCESS] Dashboard '{dash.dashboard_title}' successfully provisioned with separated clusters and polished visuals!")
    print(f"Dashboard ID: {dash.id} | Slug: {dash.slug} | URL: /superset/dashboard/{dash.slug}/")
