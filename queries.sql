-- ============================================================================
-- InfoVelocity: Real-Time Narrative Velocity & Acceleration SQL
-- Datasource: "hn_snapshots" (Apache Druid OLAP Engine)
-- Target UI: Apache Superset Dashboards & Real-Time Alerts
-- Time Column: "__time" (Ingestion snapshot observation timestamp)
-- ============================================================================
-- Precomputed Dimensions (Calculated in Python pipeline before Druid ingestion):
--   - dt_sec               : Elapsed seconds between consecutive snapshots
--   - v_score              : Score velocity in points / minute
--   - v_comment            : Comment velocity in comments / minute
--   - a_score              : Score acceleration in points / minute²
--   - a_comment            : Comment acceleration in comments / minute²
--   - a_norm               : Normalized score acceleration (a_score / max(score, 1))
--   - rank_score           : Noise-penalized velocity rank (v_score * ln(1 + score))
--   - eligible             : 1 if (score >= 2 or comments >= 1) else 0
--   - latest_flag          : 1 for latest snapshot per post_id, 0 for history
--   - title_link           : Clickable HTML story title (<a href="..." target="_blank">)
--   - story_age            : Human-readable publication age ('45m', '1.5h', '2.0d')
--   - observation_time_ist : Observation time formatted in IST ('15:43:08')
--   - quadrant             : 4-quadrant momentum classification string
--   - cluster_id           : Narrative cluster identifier ('cluster_ai', 'cluster_security')
--   - cluster_label        : Narrative cluster display label ('AI & Large Language Models')
-- ============================================================================


-- ----------------------------------------------------------------------------
-- QUERY 1: Active Stories (KPI 1)
-- ----------------------------------------------------------------------------
-- Superset Viz: Big Number with 30-min comparison (5m buckets, last 6 hours)

SELECT 
    COUNT(DISTINCT post_id) AS active_stories
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots");


-- ----------------------------------------------------------------------------
-- QUERY 2: Average & Highest Velocity (KPIs 2 & 3)
-- ----------------------------------------------------------------------------
-- Superset Viz: Big Number with 30-min comparison (active eligible stories)

SELECT
    COALESCE(ROUND(AVG(v_score), 2), 0.0) AS avg_score_velocity,
    COALESCE(ROUND(MAX(v_score), 2), 0.0) AS highest_score_velocity
FROM "hn_snapshots"
WHERE latest_flag = 1 
  AND eligible = 1 
  AND v_score IS NOT NULL;


-- ----------------------------------------------------------------------------
-- QUERY 3: Current Top Acceleration vs Peak Acceleration (last 6h) (KPIs 4 & 5)
-- ----------------------------------------------------------------------------
-- Superset Viz: Big Number with 30-min comparison

-- Current Top Acceleration (from latest snapshots):
SELECT 
    COALESCE(ROUND(MAX(a_score), 4), 0.0) AS current_top_acceleration
FROM "hn_snapshots"
WHERE latest_flag = 1 
  AND eligible = 1 
  AND a_score > 0;

-- Peak Acceleration over last 6 hours (sane capped at 50 pts/min²):
SELECT 
    COALESCE(ROUND(LEAST(MAX(CASE WHEN a_score > 0 THEN a_score END), 50.0), 4), 0.0) AS peak_acceleration_6h
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots");


-- ----------------------------------------------------------------------------
-- QUERY 4: Engagement Velocity — 5-Minute Trend (Macro Dual Time-Series)
-- ----------------------------------------------------------------------------
-- Superset Viz: echarts_timeseries_line (Score vs Comment Velocity)

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
ORDER BY bucket_time ASC;


-- ----------------------------------------------------------------------------
-- QUERY 5: Momentum Map (Kinematic 4-Quadrant Scatter / Bubble Chart)
-- ----------------------------------------------------------------------------
-- Superset Viz: bubble_v2 (x = v_score, y = a_score, size = sqrt(score), series = quadrant)

SELECT 
    title,
    author,
    quadrant,
    AVG(score_velocity_per_minute) AS x_velocity,
    AVG(score_acceleration) AS y_acceleration,
    ROUND(SQRT(GREATEST(MAX(current_score), 1)), 2) AS bubble_size_sqrt
FROM (
    SELECT
        post_id,
        title,
        author,
        score AS current_score,
        COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
        COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
        quadrant
    FROM "hn_snapshots"
    WHERE latest_flag = 1 
      AND eligible = 1
    GROUP BY post_id, title, author, score, v_score, a_score, quadrant
) sub
GROUP BY title, author, quadrant
ORDER BY x_velocity DESC
LIMIT 200;


-- ----------------------------------------------------------------------------
-- QUERY 6: Real-Time Narrative Leaderboard (Fastest Growing Stories)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Top 200, ranked by weighted rank_score DESC)

SELECT
    title_link,
    author,
    current_score,
    current_comments,
    score_velocity_per_minute,
    comment_velocity_per_minute,
    score_acceleration,
    weighted_velocity_rank,
    story_age,
    observation_time_ist
FROM (
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
        story_age,
        observation_time_ist,
        title_link,
        eligible
    FROM "hn_snapshots"
    WHERE latest_flag = 1 
      AND eligible = 1
    GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, rank_score, a_score, story_age, observation_time_ist, title_link, eligible
) sub
ORDER BY weighted_velocity_rank DESC
LIMIT 200;


-- ----------------------------------------------------------------------------
-- QUERY 7: Top Accelerating "Breakout" Stories (Positive Surge Detector)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Top 100, a_score > 0, ranked by a_norm DESC)

SELECT
    title_link,
    author,
    current_score,
    current_comments,
    score_velocity_per_minute,
    score_acceleration,
    a_norm,
    comment_acceleration,
    story_age,
    observation_time_ist
FROM (
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
        story_age,
        observation_time_ist,
        title_link
    FROM "hn_snapshots"
    WHERE latest_flag = 1 
      AND eligible = 1 
      AND a_score > 0
    GROUP BY post_id, title, author, score, num_comments, v_score, a_score, a_norm, a_comment, story_age, observation_time_ist, title_link
) sub
ORDER BY a_norm DESC
LIMIT 100;


-- ----------------------------------------------------------------------------
-- QUERY 8: Narrative Clusters (Last 6h) (NEW FEATURE)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Top 50 clusters ranked by combined velocity)

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
ORDER BY combined_velocity DESC
LIMIT 50;


-- ----------------------------------------------------------------------------
-- QUERY 9: Story Engagement Over Time (Full Trajectory Audit Table)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Observation Time DESC, row limit 500)
-- 
-- [FIX NOTE]:
--   The previous failing query contained correlated scalar subqueries inside the
--   projection ((SELECT MAX("__time") FROM "hn_snapshots") inside CONCAT / CASE),
--   which caused Druid to fail with:
--   "RUNTIME_FAILURE: Cannot convert query parts into an actual query".
--
--   FIXED by precomputing title_link, story_age, and observation_time_ist in Python.
--   Now executes as a simple scan/group-by ordered by __time DESC.

SELECT
    observation_time_ist,
    title_link,
    author,
    score,
    num_comments,
    v_score,
    a_score,
    story_age
FROM (
    SELECT
        post_id,
        title,
        author,
        score,
        num_comments,
        COALESCE(ROUND(v_score, 2), 0.0) AS v_score,
        COALESCE(ROUND(a_score, 4), 0.0) AS a_score,
        story_age,
        observation_time_ist,
        title_link,
        cluster_id,
        cluster_label,
        dt_sec,
        eligible,
        "__time"
    FROM "hn_snapshots"
    WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
    GROUP BY post_id, title, author, score, num_comments, v_score, a_score, story_age, observation_time_ist, title_link, cluster_id, cluster_label, created_at, url, dt_sec, eligible, "__time"
) AS virtual_table
ORDER BY "__time" DESC
LIMIT 500;
