-- ============================================================================
-- InfoVelocity: Real-Time Narrative Velocity & Acceleration SQL
-- Datasource: "hn_snapshots" (Apache Druid OLAP Engine)
-- Target UI: Apache Superset Dashboards & Real-Time Alerts
-- Time Column: "__time" (Ingestion snapshot observation timestamp)
-- ============================================================================
-- Physical Kinematics (Pre-computed in Python pipeline before Druid ingestion):
--   - dt_sec     : Elapsed seconds between consecutive snapshots
--   - v_score    : Score velocity in points / minute
--   - v_comment  : Comment velocity in comments / minute
--   - a_score    : Score acceleration in points / minute²
--   - a_comment  : Comment acceleration in comments / minute²
--   - a_norm     : Normalized score acceleration (a_score / max(score, 1))
--   - rank_score : Noise-penalized velocity rank (v_score * ln(1 + score))
--   - eligible   : 1 if (score >= 5 or comments >= 3) else 0
-- ============================================================================


-- ----------------------------------------------------------------------------
-- QUERY 1: Instantaneous Story Velocity (First Derivative)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Table / Scatter Plot (Score Velocity vs Comment Velocity)

SELECT 
    "__time",
    post_id,
    title,
    author,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url
        ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
    END AS url,
    score,
    num_comments,
    dt_sec,
    v_score AS score_velocity_per_minute,
    v_comment AS comment_velocity_per_minute
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
  AND v_score IS NOT NULL
ORDER BY v_score DESC
LIMIT 25;


-- ----------------------------------------------------------------------------
-- QUERY 2: Velocity Acceleration & Narrative Spurt Detection (Second Derivative)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Ranked Alert List / Acceleration Leaderboard

SELECT 
    "__time",
    post_id,
    title,
    author,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url
        ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
    END AS url,
    score,
    num_comments,
    dt_sec,
    v_score AS score_velocity_per_minute,
    v_comment AS comment_velocity_per_minute,
    a_score AS score_acceleration,
    a_comment AS comment_acceleration,
    a_norm
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1
  AND a_score IS NOT NULL
ORDER BY a_norm DESC
LIMIT 25;


-- ----------------------------------------------------------------------------
-- QUERY 3: Real-Time Narrative Leaderboard (Fastest Growing Stories)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Ranked Table / Alert Grid (Top 25)

SELECT
    post_id,
    title,
    author,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url
        ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
    END AS url,
    score AS current_score,
    num_comments AS current_comments,
    v_score AS score_velocity_per_minute,
    v_comment AS comment_velocity_per_minute,
    a_score AS score_acceleration,
    a_comment AS comment_acceleration,
    rank_score AS weighted_velocity_rank,
    "__time" AS last_observation_time
FROM "hn_snapshots"
WHERE eligible = 1
  AND latest_flag = 1
  AND v_score IS NOT NULL
ORDER BY rank_score DESC
LIMIT 25;


-- ----------------------------------------------------------------------------
-- QUERY 4: Aggregate Engagement Velocity Trend (5-Minute Time Buckets)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Time-Series Line Chart (echarts_timeseries_line)

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
WHERE v_score IS NOT NULL
GROUP BY TIME_FLOOR("__time", 'PT5M')
ORDER BY bucket_time DESC;


-- ----------------------------------------------------------------------------
-- QUERY 5: Top Accelerating "Breakout" Stories (Early Narrative Detector)
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Breakout Alert Table (Top 20)

SELECT
    post_id,
    title,
    author,
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url
        ELSE CONCAT('https://news.ycombinator.com/item?id=', post_id)
    END AS url,
    score AS current_score,
    num_comments AS current_comments,
    v_score AS score_velocity_per_minute,
    a_score AS score_acceleration,
    a_norm,
    a_comment AS comment_acceleration,
    "__time" AS detected_at
FROM "hn_snapshots"
WHERE eligible = 1
  AND latest_flag = 1
  AND a_score > 0
ORDER BY a_norm DESC
LIMIT 20;


-- ----------------------------------------------------------------------------
-- QUERY 6: KPI Executive Summary Cards
-- ----------------------------------------------------------------------------
-- Superset Chart Type: Big Number with Trendline / KPI Tile

SELECT
    COUNT(DISTINCT post_id) AS active_stories,
    ROUND(AVG(CASE WHEN v_score IS NOT NULL THEN v_score END), 2) AS avg_score_velocity,
    ROUND(MAX(v_score), 2) AS highest_score_velocity,
    COALESCE(ROUND(MAX(CASE WHEN a_score > 0 THEN a_score END), 4), 0.0) AS highest_score_acceleration
FROM "hn_snapshots"
WHERE latest_flag = 1
  AND eligible = 1;
