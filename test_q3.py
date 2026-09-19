import requests
import json

url = "http://localhost:8888/druid/v2/sql"

# Units:
# - score_velocity_per_minute = score points per minute
# - comment_velocity_per_minute = comments per minute
# - score_acceleration = score points per minute² (second derivative: delta velocity / elapsed time in minutes)
# - comment_acceleration = comments per minute² (second derivative: delta velocity / elapsed time in minutes)

q3 = """
WITH snapshot_lags AS (
    SELECT
        post_id,
        title,
        author,
        __time,
        score,
        num_comments,
        LAG(__time, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_time,
        LAG(score, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_score,
        LAG(num_comments, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_comments
    FROM "hn_snapshots"
),
velocities AS (
    SELECT
        post_id,
        title,
        author,
        __time,
        score,
        num_comments,
        (score - prev_score) AS score_delta,
        (num_comments - prev_comments) AS comment_delta,
        TIMESTAMPDIFF(SECOND, prev_time, __time) AS elapsed_seconds,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, __time) > 0 
            THEN ROUND(CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, __time) AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS score_velocity_per_minute,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, __time) > 0 
            THEN ROUND(CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, __time) AS DOUBLE) / 60.0), 4)
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
        __time,
        score,
        num_comments,
        elapsed_seconds,
        score_velocity_per_minute,
        comment_velocity_per_minute,
        LAG(score_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_score_velocity,
        LAG(comment_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY __time) AS prev_comment_velocity,
        CASE
            WHEN elapsed_seconds > 0
            THEN ROUND(
                (score_velocity_per_minute - LAG(score_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY __time))
                / (CAST(elapsed_seconds AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS score_acceleration,
        CASE
            WHEN elapsed_seconds > 0
            THEN ROUND(
                (comment_velocity_per_minute - LAG(comment_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY __time))
                / (CAST(elapsed_seconds AS DOUBLE) / 60.0),
                6
            )
            ELSE 0.0
        END AS comment_acceleration
    FROM velocities
)
SELECT 
    post_id,
    title,
    author,
    score,
    num_comments,
    elapsed_seconds,
    score_velocity_per_minute,
    comment_velocity_per_minute,
    score_acceleration,
    comment_acceleration,
    __time
FROM accelerations
WHERE prev_score_velocity IS NOT NULL
ORDER BY score_acceleration DESC
LIMIT 5
"""

res = requests.post(url, json={"query": q3})
print("Q3 Status:", res.status_code)
if res.status_code == 200:
    data = res.json()
    print(f"Success! {len(data)} rows.")
    if data:
        print(json.dumps(data, indent=2))
else:
    print(res.text)
