import requests
import json

url = "http://localhost:8888/druid/v2/sql"

q4 = """
WITH snapshot_lags AS (
    SELECT
        post_id,
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
        author,
        __time,
        TIMESTAMPDIFF(SECOND, prev_time, __time) AS elapsed_seconds,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, __time) > 0 
            THEN CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, __time) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS score_velocity_per_minute,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, __time) > 0 
            THEN CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, __time) AS DOUBLE) / 60.0)
            ELSE 0.0
        END AS comment_velocity_per_minute
    FROM snapshot_lags
)
SELECT
    TIME_FLOOR(__time, 'PT5M') AS bucket_time,
    COUNT(*) AS total_snapshot_observations,
    COUNT(DISTINCT post_id) AS unique_stories,
    COUNT(DISTINCT author) AS unique_authors,
    ROUND(AVG(score_velocity_per_minute), 4) AS avg_score_velocity,
    ROUND(AVG(comment_velocity_per_minute), 4) AS avg_comment_velocity
FROM velocities
GROUP BY TIME_FLOOR(__time, 'PT5M')
ORDER BY bucket_time DESC
"""

res = requests.post(url, json={"query": q4})
print("Q4 Status:", res.status_code)
if res.status_code == 200:
    data = res.json()
    print(f"Success! {len(data)} buckets.")
    print(json.dumps(data, indent=2))
else:
    print(res.text)
