from superset.app import create_app
from sqlalchemy import text

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database
    druid_db = db.session.query(Database).filter_by(id=2).one()

    sql = """
    WITH snapshot_lags AS (
        SELECT
            post_id,
            author,
            "__time",
            score,
            num_comments,
            LAG("__time", 1) OVER (PARTITION BY post_id ORDER BY "__time") AS prev_time,
            LAG(score, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS prev_score,
            LAG(num_comments, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS prev_comments
        FROM "hn_snapshots"
    ),
    velocities AS (
        SELECT
            post_id,
            author,
            "__time",
            TIMESTAMPDIFF(SECOND, prev_time, "__time") AS elapsed_seconds,
            CASE 
                WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
                THEN CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0)
                ELSE 0.0
            END AS score_velocity_per_minute,
            CASE 
                WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
                THEN CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0)
                ELSE 0.0
            END AS comment_velocity_per_minute
        FROM snapshot_lags
        WHERE prev_time IS NOT NULL
    )
    SELECT
        TIME_FLOOR("__time", 'PT5M') AS bucket_time,
        COUNT(*) AS total_snapshot_observations,
        COUNT(DISTINCT post_id) AS active_tracked_stories,
        COUNT(DISTINCT author) AS active_authors,
        ROUND(AVG(score_velocity_per_minute), 4) AS avg_score_velocity,
        ROUND(MAX(score_velocity_per_minute), 4) AS peak_score_velocity,
        ROUND(AVG(comment_velocity_per_minute), 4) AS avg_comment_velocity,
        ROUND(MAX(comment_velocity_per_minute), 4) AS peak_comment_velocity
    FROM velocities
    GROUP BY TIME_FLOOR("__time", 'PT5M')
    ORDER BY bucket_time ASC
    """

    print("Connecting via engine context...")
    with druid_db.get_sqla_engine() as eng:
        with eng.connect() as conn:
            res = conn.execute(text(sql))
            rows = res.fetchall()
            print("Success! Rows:", len(rows))
            for r in rows[:2]:
                print(r)
