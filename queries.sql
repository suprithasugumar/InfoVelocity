-- ============================================================================
-- InfoVelocity: Real-Time Narrative Velocity & Acceleration SQL
-- Datasource: "hn_snapshots" (Apache Druid OLAP Engine)
-- Target UI: Apache Superset Dashboards & Real-Time Alerts
-- Time Column: "__time" (Ingestion snapshot observation timestamp)
-- ============================================================================
-- Units & Physical Dimensions:
--   - score_velocity_per_minute   : score points per minute
--   - comment_velocity_per_minute : comments per minute
--   - score_acceleration          : score points per minute²  (Δvelocity / Δt)
--   - comment_acceleration        : comments per minute²      (Δvelocity / Δt)
--
-- Mathematical Model:
--   1. Instantaneous Velocity (v):
--      v = ΔEngagement / Δt
--      score_velocity_per_minute = (score_t - score_{t-1}) / ((t - t_{t-1}) / 60.0)
--      comment_velocity_per_minute = (comments_t - comments_{t-1}) / ((t - t_{t-1}) / 60.0)
--
--   2. True Acceleration as Second Derivative (a):
--      a = ΔVelocity / Δt = (v_t - v_{t-1}) / ((t - t_{t-1}) / 60.0)
--      score_acceleration = (score_velocity_t - score_velocity_{t-1}) / (elapsed_seconds / 60.0)
--      comment_acceleration = (comment_velocity_t - comment_velocity_{t-1}) / (elapsed_seconds / 60.0)
--
-- URL Formatting Rule:
--   Every displayed URL is guaranteed to be either:
--   1. The original Hacker News item's external URL, or
--   2. https://news.ycombinator.com/item?id={post_id}
-- ============================================================================


-- ----------------------------------------------------------------------------
-- QUERY 1: Instantaneous Story Velocity (First Derivative)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Table / Scatter Plot (Score Velocity vs Comment Velocity)
-- Units:
--   score_velocity_per_minute   = score points per minute
--   comment_velocity_per_minute = comments per minute

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
        (score - prev_score) AS score_delta,
        (num_comments - prev_comments) AS comment_delta,
        TIMESTAMPDIFF(SECOND, prev_time, "__time") AS elapsed_seconds,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
            THEN ROUND(CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS score_velocity_per_minute,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
            THEN ROUND(CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS comment_velocity_per_minute
    FROM snapshot_lags
    WHERE prev_time IS NOT NULL
)
SELECT 
    "__time",
    post_id,
    title,
    author,
    url,
    score,
    num_comments,
    score_delta,
    comment_delta,
    elapsed_seconds,
    score_velocity_per_minute,
    comment_velocity_per_minute
FROM velocities
ORDER BY score_velocity_per_minute DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: Velocity Acceleration & Narrative Spurt Detection (Second Derivative)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Time-Series Line Chart / Bar Chart
-- Units:
--   score_velocity_per_minute   = score points per minute
--   comment_velocity_per_minute = comments per minute
--   score_acceleration          = score points per minute²
--   comment_acceleration        = comments per minute²

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
        (score - prev_score) AS score_delta,
        (num_comments - prev_comments) AS comment_delta,
        TIMESTAMPDIFF(SECOND, prev_time, "__time") AS elapsed_seconds,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
            THEN ROUND(CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS score_velocity_per_minute,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
            THEN ROUND(CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
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
        LAG(score_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS prev_score_velocity,
        LAG(comment_velocity_per_minute, 1) OVER (PARTITION BY post_id ORDER BY "__time") AS prev_comment_velocity,
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
        END AS comment_acceleration
    FROM velocities
)
SELECT 
    "__time",
    post_id,
    title,
    author,
    url,
    score,
    num_comments,
    elapsed_seconds,
    score_velocity_per_minute,
    comment_velocity_per_minute,
    score_acceleration,
    comment_acceleration
FROM accelerations
WHERE prev_score_velocity IS NOT NULL
ORDER BY score_acceleration DESC;


-- ----------------------------------------------------------------------------
-- QUERY 3: Real-Time Narrative Leaderboard & Surge Alert (Latest Snapshot)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Ranked Table / Alert Grid
-- Units:
--   score_velocity_per_minute   = score points per minute
--   comment_velocity_per_minute = comments per minute
--   score_acceleration          = score points per minute²
--   comment_acceleration        = comments per minute²

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
            THEN ROUND(CAST(score - prev_score AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
            ELSE 0.0
        END AS score_velocity_per_minute,
        CASE 
            WHEN prev_time IS NOT NULL AND TIMESTAMPDIFF(SECOND, prev_time, "__time") > 0 
            THEN ROUND(CAST(num_comments - prev_comments AS DOUBLE) / (CAST(TIMESTAMPDIFF(SECOND, prev_time, "__time") AS DOUBLE) / 60.0), 4)
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
    score_velocity_per_minute,
    comment_velocity_per_minute,
    score_acceleration,
    comment_acceleration,
    "__time" AS last_observation_time
FROM accelerations
WHERE recency_rank = 1
ORDER BY score_velocity_per_minute DESC, score_acceleration DESC
LIMIT 25;


-- ----------------------------------------------------------------------------
-- QUERY 4: Macro Narrative Velocity Time-Series (Bucketed Aggregation)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Time-Series Area Chart / Multi-line Chart
-- Units:
--   avg_score_velocity          = score points per minute
--   avg_comment_velocity        = comments per minute

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
ORDER BY bucket_time DESC;


-- ----------------------------------------------------------------------------
-- QUERY 5: Top Accelerating "Breakout" Stories (Early Narrative Detector)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Horizon Chart / Bullet Chart / Ranked Alert List
-- Units:
--   score_velocity_per_minute   = score points per minute
--   score_acceleration          = score points per minute²

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
LIMIT 15;
