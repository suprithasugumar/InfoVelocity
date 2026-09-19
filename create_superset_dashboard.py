"""
Superset Automation Script for InfoVelocity Stage 8B (Fix Axis Overlap & Full Table Height)
==========================================================================================
Fixes:
1. Fix Trend Graph axis & wording overlap:
   - x_axis_time_format: 'smart_date' (clean, non-colliding time stamps)
   - Remove redundant colliding x_axis_title
   - Set y_axis_title_margin: 35 and y_axis_title: 'Velocity (pts/min)'
2. Fix Breakout Monitor showing only 1 news row:
   - Increased container height from 28 to 56 so all 9+ breakout stories are fully visible on screen
   - Ensured page_size=10 and row_limit=20
3. Fix all chart container heights to 56 for optimal visibility without vertical truncation.
4. Preserved full-cell conditional colors (#1b4d2e green for positive, #5c1d1d red for negative).
5. 10-second auto-refresh enabled.
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
    # 1. Datasets (Preserve exact SQL definitions)
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
    sql_macro = """
WITH snapshot_lags AS (
    SELECT
        post_id AS POST_ID,
        author AS AUTHOR,
        "__time" AS OBS_TIME,
        score AS SCORE,
        num_comments AS NUM_COMMENTS,
        LAG("__time", 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_TIME,
        LAG(score, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_SCORE,
        LAG(num_comments, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_COMMENTS
    FROM "hn_snapshots"
),
velocities AS (
    SELECT
        POST_ID,
        AUTHOR,
        OBS_TIME,
        TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS ELAPSED_SECONDS,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN CAST(SCORE - PREV_SCORE AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS SCORE_VELOCITY_PER_MINUTE,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN CAST(NUM_COMMENTS - PREV_COMMENTS AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS COMMENT_VELOCITY_PER_MINUTE
    FROM snapshot_lags
    WHERE PREV_TIME IS NOT NULL
)
SELECT
    TIME_FLOOR(OBS_TIME, 'PT5M') AS bucket_time,
    COUNT(*) AS total_snapshot_observations,
    COUNT(DISTINCT POST_ID) AS active_tracked_stories,
    COUNT(DISTINCT AUTHOR) AS active_authors,
    ROUND(AVG(SCORE_VELOCITY_PER_MINUTE), 4) AS avg_score_velocity,
    ROUND(MAX(SCORE_VELOCITY_PER_MINUTE), 4) AS peak_score_velocity,
    ROUND(AVG(COMMENT_VELOCITY_PER_MINUTE), 4) AS avg_comment_velocity,
    ROUND(MAX(COMMENT_VELOCITY_PER_MINUTE), 4) AS peak_comment_velocity
FROM velocities
GROUP BY TIME_FLOOR(OBS_TIME, 'PT5M')
ORDER BY bucket_time ASC
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

    # 1c. Virtual Dataset: hn_fastest_growing_stories
    sql_fastest = """
WITH snapshot_lags AS (
    SELECT
        post_id AS POST_ID,
        title AS TITLE,
        author AS AUTHOR,
        CASE 
            WHEN url IS NOT NULL AND url != '' THEN url
            ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
        END AS URL,
        "__time" AS OBS_TIME,
        score AS SCORE,
        num_comments AS NUM_COMMENTS,
        LAG("__time", 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_TIME,
        LAG(score, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_SCORE,
        LAG(num_comments, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_COMMENTS
    FROM "hn_snapshots"
),
velocities AS (
    SELECT
        POST_ID,
        TITLE,
        AUTHOR,
        URL,
        OBS_TIME,
        SCORE,
        NUM_COMMENTS,
        TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS ELAPSED_SECONDS,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN ROUND(CAST(SCORE - PREV_SCORE AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS SCORE_VELOCITY_PER_MINUTE,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN ROUND(CAST(NUM_COMMENTS - PREV_COMMENTS AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS COMMENT_VELOCITY_PER_MINUTE
    FROM snapshot_lags
    WHERE PREV_TIME IS NOT NULL
),
accelerations AS (
    SELECT
        POST_ID,
        TITLE,
        AUTHOR,
        URL,
        OBS_TIME,
        SCORE,
        NUM_COMMENTS,
        ELAPSED_SECONDS,
        SCORE_VELOCITY_PER_MINUTE,
        COMMENT_VELOCITY_PER_MINUTE,
        CASE
            WHEN ELAPSED_SECONDS > 0 AND LAG(SCORE_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME) IS NOT NULL
            THEN ROUND(
                (SCORE_VELOCITY_PER_MINUTE - LAG(SCORE_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME))
                / (CAST(ELAPSED_SECONDS AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS SCORE_ACCELERATION,
        CASE
            WHEN ELAPSED_SECONDS > 0 AND LAG(COMMENT_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME) IS NOT NULL
            THEN ROUND(
                (COMMENT_VELOCITY_PER_MINUTE - LAG(COMMENT_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME))
                / (CAST(ELAPSED_SECONDS AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS COMMENT_ACCELERATION,
        ROW_NUMBER() OVER (PARTITION BY POST_ID ORDER BY OBS_TIME DESC) AS RECENCY_RANK
    FROM velocities
)
SELECT
    POST_ID AS post_id,
    TITLE AS title,
    AUTHOR AS author,
    URL AS url,
    SCORE AS current_score,
    NUM_COMMENTS AS current_comments,
    SCORE_VELOCITY_PER_MINUTE AS score_velocity_per_minute,
    COMMENT_VELOCITY_PER_MINUTE AS comment_velocity_per_minute,
    SCORE_ACCELERATION AS score_acceleration,
    COMMENT_ACCELERATION AS comment_acceleration,
    OBS_TIME AS last_observation_time
FROM accelerations
WHERE RECENCY_RANK = 1
ORDER BY score_velocity_per_minute DESC, score_acceleration DESC
""".strip()

    ds_fastest = db.session.query(SqlaTable).filter_by(
        database_id=druid_db.id, table_name="hn_fastest_growing_stories"
    ).first()
    if not ds_fastest:
        ds_fastest = SqlaTable(
            table_name="hn_fastest_growing_stories",
            database_id=druid_db.id,
            sql=sql_fastest,
            owners=[admin_user],
            main_dttm_col="last_observation_time",
        )
        db.session.add(ds_fastest)
        db.session.flush()
        print(f"Created virtual dataset hn_fastest_growing_stories (ID: {ds_fastest.id})")
    else:
        ds_fastest.sql = sql_fastest
        print(f"Updated virtual dataset hn_fastest_growing_stories (ID: {ds_fastest.id})")

    # 1d. Virtual Dataset: hn_breakout_accelerations
    sql_breakout = """
WITH snapshot_lags AS (
    SELECT
        post_id AS POST_ID,
        title AS TITLE,
        author AS AUTHOR,
        CASE 
            WHEN url IS NOT NULL AND url != '' THEN url
            ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
        END AS URL,
        "__time" AS OBS_TIME,
        score AS SCORE,
        num_comments AS NUM_COMMENTS,
        LAG("__time", 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_TIME,
        LAG(score, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_SCORE,
        LAG(num_comments, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS PREV_COMMENTS
    FROM "hn_snapshots"
),
velocities AS (
    SELECT
        POST_ID,
        TITLE,
        AUTHOR,
        URL,
        OBS_TIME,
        SCORE,
        NUM_COMMENTS,
        TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS ELAPSED_SECONDS,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN CAST(SCORE - PREV_SCORE AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS SCORE_VELOCITY_PER_MINUTE,
        CASE 
            WHEN PREV_TIME IS NOT NULL AND TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) > 0 
            THEN CAST(NUM_COMMENTS - PREV_COMMENTS AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, PREV_TIME, OBS_TIME) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS COMMENT_VELOCITY_PER_MINUTE
    FROM snapshot_lags
    WHERE PREV_TIME IS NOT NULL
),
accelerations AS (
    SELECT
        POST_ID,
        TITLE,
        AUTHOR,
        URL,
        OBS_TIME,
        SCORE,
        NUM_COMMENTS,
        ELAPSED_SECONDS,
        SCORE_VELOCITY_PER_MINUTE,
        COMMENT_VELOCITY_PER_MINUTE,
        CASE
            WHEN ELAPSED_SECONDS > 0 AND LAG(SCORE_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME) IS NOT NULL
            THEN ROUND(
                (SCORE_VELOCITY_PER_MINUTE - LAG(SCORE_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME))
                / (CAST(ELAPSED_SECONDS AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS SCORE_ACCELERATION,
        CASE
            WHEN ELAPSED_SECONDS > 0 AND LAG(COMMENT_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME) IS NOT NULL
            THEN ROUND(
                (COMMENT_VELOCITY_PER_MINUTE - LAG(COMMENT_VELOCITY_PER_MINUTE, 1) OVER (PARTITION BY POST_ID ORDER BY OBS_TIME))
                / (CAST(ELAPSED_SECONDS AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS COMMENT_ACCELERATION,
        ROW_NUMBER() OVER (PARTITION BY POST_ID ORDER BY OBS_TIME DESC) AS RECENCY_RANK
    FROM velocities
)
SELECT
    POST_ID AS post_id,
    TITLE AS title,
    AUTHOR AS author,
    URL AS url,
    SCORE AS current_score,
    NUM_COMMENTS AS current_comments,
    ROUND(SCORE_VELOCITY_PER_MINUTE, 4) AS score_velocity_per_minute,
    ROUND(SCORE_ACCELERATION, 6) AS score_acceleration,
    ROUND(COMMENT_ACCELERATION, 6) AS comment_acceleration,
    OBS_TIME AS detected_at
FROM accelerations
WHERE RECENCY_RANK = 1 
  AND SCORE_ACCELERATION > 0.0
ORDER BY score_acceleration DESC
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

    db.session.commit()

    # Apply standardized column verbose labels across all datasets
    column_labels = {
        "title": "Story Title",
        "author": "Author",
        "url": "URL",
        "current_score": "Score",
        "current_comments": "Comments",
        "score": "Score",
        "num_comments": "Comments",
        "score_velocity_per_minute": "Engagement Velocity (pts/min)",
        "comment_velocity_per_minute": "Comment Velocity (comments/min)",
        "score_acceleration": "Score Acceleration (pts/min²)",
        "comment_acceleration": "Comment Acceleration (comments/min²)",
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

    for ds in [ds_physical, ds_macro, ds_fastest, ds_breakout]:
        try:
            ds.fetch_metadata()
            db.session.commit()
            for col in ds.columns:
                if col.column_name in column_labels:
                    col.verbose_name = column_labels[col.column_name]
            db.session.commit()
            print(f"Metadata and verbose labels refreshed for {ds.table_name}")
        except Exception as e:
            print(f"Metadata fetch warning for {ds.table_name}: {e}")

    # -------------------------------------------------------------------------
    # 2. KPI Summary Cards (Row 1 across full width)
    # -------------------------------------------------------------------------
    kpi_slices = []

    # KPI 1: Active Stories
    kpi1_name = "Active Stories"
    kpi1_params = {
        "datasource": f"{ds_fastest.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(DISTINCT post_id)",
            "label": "Active Stories"
        },
        "adhoc_filters": [],
        "subheader": "Currently Tracked Narratives",
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
            datasource_id=ds_fastest.id,
            params=json.dumps(kpi1_params),
            owners=[admin_user],
            description="Total count of active distinct stories currently tracked in the narrative pool."
        )
        db.session.add(kpi1)
    else:
        kpi1.params = json.dumps(kpi1_params)
        kpi1.datasource_id = ds_fastest.id
    db.session.flush()
    kpi_slices.append(kpi1)
    print(f"KPI 1 '{kpi1_name}' ready (ID: {kpi1.id})")

    # KPI 2: Average Score Velocity
    kpi2_name = "Average Score Velocity"
    kpi2_params = {
        "datasource": f"{ds_fastest.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "ROUND(AVG(score_velocity_per_minute), 2)",
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
            datasource_id=ds_fastest.id,
            params=json.dumps(kpi2_params),
            owners=[admin_user],
            description="Mean score velocity across all actively tracked stories in the current observation cycle."
        )
        db.session.add(kpi2)
    else:
        kpi2.params = json.dumps(kpi2_params)
        kpi2.datasource_id = ds_fastest.id
    db.session.flush()
    kpi_slices.append(kpi2)
    print(f"KPI 2 '{kpi2_name}' ready (ID: {kpi2.id})")

    # KPI 3: Highest Score Velocity
    kpi3_name = "Highest Score Velocity"
    kpi3_params = {
        "datasource": f"{ds_fastest.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "ROUND(MAX(score_velocity_per_minute), 2)",
            "label": "Peak Velocity"
        },
        "adhoc_filters": [],
        "subheader": "Engagement Velocity (pts/min)",
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
            datasource_id=ds_fastest.id,
            params=json.dumps(kpi3_params),
            owners=[admin_user],
            description="Peak score velocity among active stories in the current observation cycle."
        )
        db.session.add(kpi3)
    else:
        kpi3.params = json.dumps(kpi3_params)
        kpi3.datasource_id = ds_fastest.id
    db.session.flush()
    kpi_slices.append(kpi3)
    print(f"KPI 3 '{kpi3_name}' ready (ID: {kpi3.id})")

    # KPI 4: Highest Positive Acceleration
    kpi4_name = "Highest Positive Acceleration"
    kpi4_params = {
        "datasource": f"{ds_breakout.id}__table",
        "viz_type": "big_number_total",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COALESCE(ROUND(MAX(score_acceleration), 4), 0.0)",
            "label": "Peak Acceleration"
        },
        "adhoc_filters": [],
        "subheader": "Score Acceleration (pts/min²)",
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
            datasource_id=ds_breakout.id,
            params=json.dumps(kpi4_params),
            owners=[admin_user],
            description="Maximum instantaneous acceleration surge detected among breakout stories."
        )
        db.session.add(kpi4)
    else:
        kpi4.params = json.dumps(kpi4_params)
        kpi4.datasource_id = ds_breakout.id
    db.session.flush()
    kpi_slices.append(kpi4)
    print(f"KPI 4 '{kpi4_name}' ready (ID: {kpi4.id})")

    # -------------------------------------------------------------------------
    # 3. Core Charts (Slices with clean axis formatting & full cell colors)
    # -------------------------------------------------------------------------
    core_slices = []

    # Chart 1: Engagement Velocity — 5-Minute Trend
    # Fix: smart_date time format, no overlapping axis titles, clear margin
    slice1_name = "Engagement Velocity — 5-Minute Trend"
    old_slice1_name = "Velocity Trend (5-min Buckets: pts/min & comments/min)"
    slice1_params = {
        "datasource": f"{ds_macro.id}__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "bucket_time",
        "x_axis_time_format": "smart_date",
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
        "order_desc": True,
        "row_limit": 100,
        "seriesType": "line",
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_title": "",
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

    # Chart 2: Fastest Growing Narratives (Table)
    slice2_name = "Fastest Growing Narratives"
    old_slice2_name = "Fastest Growing Stories (Real-Time Velocity Leaderboard)"
    slice2_params = {
        "datasource": f"{ds_fastest.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "title",
            "author",
            "current_score",
            "current_comments",
            "score_velocity_per_minute",
            "comment_velocity_per_minute",
            "score_acceleration",
            "comment_acceleration",
            "url"
        ],
        "order_by_cols": ['["score_velocity_per_minute", false]'],
        "row_limit": 25,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "table_timestamp_format": "smart_date",
        "page_size": 10,
        "server_page_length": 10,
        "column_config": {
            "title": {"columnWidth": 220},
            "author": {"columnWidth": 90},
            "current_score": {"columnWidth": 65},
            "current_comments": {"columnWidth": 75},
            "score_velocity_per_minute": {"columnWidth": 115, "d3NumberFormat": ",.2f"},
            "comment_velocity_per_minute": {"columnWidth": 115, "d3NumberFormat": ",.2f"},
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
                "column": "comment_velocity_per_minute",
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
            datasource_id=ds_fastest.id,
            params=json.dumps(slice2_params),
            owners=[admin_user],
            description="Real-time leaderboard ranking stories by score velocity (pts/min) and comment velocity (comments/min) with direct story URLs."
        )
        db.session.add(s2)
    else:
        s2.slice_name = slice2_name
        s2.params = json.dumps(slice2_params)
        s2.datasource_id = ds_fastest.id
    db.session.flush()
    core_slices.append(s2)
    print(f"Chart 2 '{slice2_name}' ready (ID: {s2.id})")

    # Chart 3: Breakout Monitor — Positive Acceleration (Table)
    slice3_name = "Breakout Monitor — Positive Acceleration"
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
            "comment_acceleration",
            "detected_at",
            "url"
        ],
        "order_by_cols": ['["score_acceleration", false]'],
        "row_limit": 25,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 10,
        "server_page_length": 10,
        "column_config": {
            "title": {"columnWidth": 380},
            "author": {"columnWidth": 110},
            "current_score": {"columnWidth": 75},
            "current_comments": {"columnWidth": 85},
            "score_velocity_per_minute": {"columnWidth": 135, "d3NumberFormat": ",.2f"},
            "score_acceleration": {"columnWidth": 145, "d3NumberFormat": ",.6f"},
            "comment_acceleration": {"columnWidth": 145, "d3NumberFormat": ",.6f"},
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
            description="Early breakout alert monitor: surfaces stories with positive score acceleration (second derivative > 0 pts/min²)."
        )
        db.session.add(s3)
    else:
        s3.slice_name = slice3_name
        s3.params = json.dumps(slice3_params)
        s3.datasource_id = ds_breakout.id
    db.session.flush()
    core_slices.append(s3)
    print(f"Chart 3 '{slice3_name}' ready (ID: {s3.id})")

    # Chart 4: Story Engagement Over Time (Table)
    slice4_name = "Story Engagement Over Time"
    old_slice4_name = "Story Engagement Progression (Historical Snapshots Log)"
    slice4_params = {
        "datasource": f"{ds_physical.id}__table",
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": [
            "__time",
            "title",
            "author",
            "score",
            "num_comments"
        ],
        "order_by_cols": ['["__time", false]'],
        "row_limit": 50,
        "show_cell_bars": False,
        "align_pn": False,
        "color_pn": False,
        "include_search": True,
        "page_size": 10,
        "server_page_length": 10,
        "column_config": {
            "__time": {"columnWidth": 170},
            "title": {"columnWidth": 450},
            "author": {"columnWidth": 130},
            "score": {"columnWidth": 85},
            "num_comments": {"columnWidth": 95}
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
            datasource_id=ds_physical.id,
            params=json.dumps(slice4_params),
            owners=[admin_user],
            description="Time-series engagement log displaying raw score and comment growth per story across successive observation snapshots."
        )
        db.session.add(s4)
    else:
        s4.slice_name = slice4_name
        s4.params = json.dumps(slice4_params)
        s4.datasource_id = ds_physical.id
    db.session.flush()
    core_slices.append(s4)
    print(f"Chart 4 '{slice4_name}' ready (ID: {s4.id})")

    all_dashboard_slices = kpi_slices + core_slices
    db.session.commit()

    # -------------------------------------------------------------------------
    # 4. Premium Dashboard Layout & Metadata Construction
    # -------------------------------------------------------------------------
    dash_title = "InfoVelocity – Real-Time Narrative Intelligence"
    dash = db.session.query(Dashboard).filter(
        (Dashboard.dashboard_title == dash_title) | 
        (Dashboard.dashboard_title == "InfoVelocity – Real-Time Misinformation Velocity Tracker") |
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
        "## ⚡ InfoVelocity\n"
        "### Real-Time Narrative Intelligence\n"
        "*Monitor how quickly online narratives gain engagement momentum.* &nbsp;&nbsp; "
        "`🔴 LIVE` &nbsp; `⏱️ 10s REFRESH` &nbsp; `📡 HACKER NEWS API`"
    )

    # Disclaimer Banner: Compact full-width banner with expandable details
    disclaimer_text = (
        "> ⚠️ **VELOCITY ≠ VERACITY** &nbsp;—&nbsp; **Rapid engagement indicates information diffusion and momentum, not factual accuracy.**\n"
        ">\n"
        "> <details><summary><b>ℹ️ Epistemic Scope & Kinematic Model Details (Click to Expand)</b></summary>\n"
        ">\n"
        "> **InfoVelocity** observes real-time engagement dynamics across Hacker News discussions, calculating:\n"
        "> - **Instantaneous Velocity ($v = \\Delta \\text{Engagement}/\\Delta t$)**: Rate of score & comment growth in **points/min** and **comments/min**.\n"
        "> - **True Acceleration ($a = \\Delta v/\\Delta t$)**: Rate of change in velocity in **points/min²** and **comments/min²** to detect early breakout narratives.\n"
        ">\n"
        "> High velocity and positive acceleration reflect viral surge dynamics across the social graph, not factual truth or credibility.\n"
        "> </details>"
    )

    # Full height 56 ensures all 10 rows in Breakout Monitor, Fastest Growing, and Story Progression are fully displayed without clipping!
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

    dash.position_json = json.dumps(pos_layout)
    dash.json_metadata = json.dumps({
        "color_scheme": "supersetAndPresetColors",
        "refresh_frequency": 10,
        "timed_refresh_immune_slices": []
    })

    db.session.commit()
    print(f"\n[SUCCESS] Dashboard '{dash.dashboard_title}' successfully provisioned with clean axis formatting & full table height!")
    print(f"Dashboard ID: {dash.id} | Slug: {dash.slug} | URL: /superset/dashboard/{dash.slug}/")
