import requests
import json

url = "http://localhost:8888/druid/v2/sql"

q2 = """
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
)
SELECT * FROM velocities
ORDER BY score_velocity_per_minute DESC
LIMIT 5
"""

res = requests.post(url, json={"query": q2})
print("Q2 Status:", res.status_code)
if res.status_code == 200:
    print(json.dumps(res.json()[:2], indent=2))
else:
    print(res.text)
