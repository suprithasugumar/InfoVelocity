import requests
import json

url = "http://localhost:8888/druid/v2/sql"

sql = """
WITH snapshot_lags AS (
    SELECT
        post_id,
        title,
        author,
        CASE 
            WHEN url IS NOT NULL AND url != '' THEN url
            ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
        END AS url,
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
        title,
        author,
        url,
        "__time",
        score,
        num_comments,
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
),
accelerations AS (
    SELECT
        post_id,
        title,
        author,
        url,
        "__time",
        score,
        num_comments,
        elapsed_seconds,
        score_velocity_per_minute,
        comment_velocity_per_minute,
        CASE
            WHEN elapsed_seconds > 0 AND LAG(score_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time") IS NOT NULL
            THEN ROUND(
                (score_velocity_per_minute - LAG(score_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time"))
                / (CAST(elapsed_seconds AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS score_acceleration,
        CASE
            WHEN elapsed_seconds > 0 AND LAG(comment_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time") IS NOT NULL
            THEN ROUND(
                (comment_velocity_per_minute - LAG(comment_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time"))
                / (CAST(elapsed_seconds AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS comment_acceleration,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY "__time" DESC) AS recency_rank
    FROM velocities
)
SELECT
    post_id,
    title,
    author,
    url,
    score AS current_score,
    num_comments AS current_comments,
    ROUND(score_velocity_per_minute, 4) AS score_velocity_per_minute,
    ROUND(score_acceleration, 6) AS score_acceleration,
    ROUND(comment_acceleration, 6) AS comment_acceleration,
    "__time" AS detected_at
FROM accelerations
WHERE recency_rank = 1 
  AND score_acceleration > 0.0
ORDER BY score_acceleration DESC
LIMIT 5
"""

r = requests.post(url, json={"query": sql})
print("Status:", r.status_code)
for row in r.json():
    print(f"ID: {row['post_id']} | Title: {row['title'][:35]} | URL: {row['url']}")
