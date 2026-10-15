-- ============================================================================
-- InfoVelocity: Real-Time Narrative Velocity & Acceleration SQL
-- Datasource: "hn_snapshots" (Apache Druid OLAP Engine)
-- Target UI: Apache Superset Dashboards & Real-Time Alerts
-- Time Column: "__time" (Ingestion snapshot observation timestamp)
-- ============================================================================
-- Physical Kinematics (Pre-computed in Python pipeline before Druid ingestion):
--   - dt_sec     : Elapsed seconds between consecutive snapshots (min dt 20s, max 3*poll)
--   - v_score    : Score velocity in points / minute
--   - v_comment  : Comment velocity in comments / minute
--   - a_score    : Score acceleration in points / minute²
--   - a_comment  : Comment acceleration in comments / minute²
--   - a_norm     : Normalized score acceleration (a_score / max(score, 1))
--   - rank_score : Noise-penalized velocity rank (v_score * ln(1 + score))
--   - eligible   : 1 if (score >= 2 or comments >= 1) else 0
--   - latest_flag: 1 for latest snapshot per post_id, 0 for history
-- ============================================================================


-- ----------------------------------------------------------------------------
-- QUERY 1: Active Stories (KPI 1)
-- ----------------------------------------------------------------------------
-- Superset Viz: Big Number with Trendline (5m buckets, last 6 hours)

SELECT 
    COUNT(DISTINCT post_id) AS active_stories
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots");


-- ----------------------------------------------------------------------------
-- QUERY 2: Average & Highest Velocity (KPIs 2 & 3)
-- ----------------------------------------------------------------------------
-- Superset Viz: Big Number with Trendline (last 6 hours, active eligible stories)

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
-- Superset Viz: Big Number Total

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
-- Superset Viz: bubble_v2 (x = v_score, y = a_score, size = score, series = quadrant)

SELECT 
    title,
    author,
    quadrant,
    AVG(score_velocity_per_minute) AS x_velocity,
    AVG(score_acceleration) AS y_acceleration,
    MAX(current_score) AS bubble_size
FROM (
    SELECT
        post_id,
        title,
        author,
        score AS current_score,
        COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
        COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
        CASE 
            WHEN v_score > 0 AND a_score > 0 THEN 'Breakout (Accelerating)'
            WHEN v_score > 0 AND a_score < 0 THEN 'Cooling (Slowing)'
            WHEN v_score > 0 AND (a_score = 0 OR a_score IS NULL) THEN 'Steady (Linear Growth)'
            ELSE 'Fading / Dormant'
        END AS quadrant
    FROM "hn_snapshots"
    WHERE latest_flag = 1 
      AND eligible = 1
    GROUP BY post_id, title, author, score, v_score, a_score
) sub
GROUP BY title, author, quadrant
ORDER BY x_velocity DESC
LIMIT 100;


-- ----------------------------------------------------------------------------
-- QUERY 6: Real-Time Narrative Leaderboard (Fastest Growing Stories)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Top 100, ranked by weighted rank_score)

SELECT
    '<a href="' || 
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url 
        ELSE 'https://news.ycombinator.com/item?id=' || post_id 
    END || 
    '" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">' || 
    title || 
    '</a>' AS title_link,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(v_comment, 2), 0.0) AS comment_velocity_per_minute,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(rank_score, 4), 0.0) AS weighted_velocity_rank,
    CASE 
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 3600 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 60.0) AS VARCHAR), 'm')
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 86400 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 3600.0, 1) AS VARCHAR), 'h')
        ELSE CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 86400.0, 1) AS VARCHAR), 'd')
    END AS story_age,
    CONCAT(
        TIME_FORMAT("__time", 'HH:mm:ss', 'Asia/Kolkata'),
        ' (',
        CASE 
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 60 
                THEN 'just now'
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 3600 
                THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 60.0) AS VARCHAR), 'm ago')
            ELSE 
                CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 3600.0, 1) AS VARCHAR), 'h ago')
        END,
        ')'
    ) AS observation_time_ist
FROM "hn_snapshots"
WHERE latest_flag = 1 
  AND eligible = 1
GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, rank_score, a_score, created_at, url, "__time"
ORDER BY weighted_velocity_rank DESC
LIMIT 100;


-- ----------------------------------------------------------------------------
-- QUERY 7: Top Accelerating "Breakout" Stories (Positive Surge Detector)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Top 100, a_score > 0, ranked by a_norm DESC)

SELECT
    '<a href="' || 
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url 
        ELSE 'https://news.ycombinator.com/item?id=' || post_id 
    END || 
    '" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">' || 
    title || 
    '</a>' AS title_link,
    author,
    score AS current_score,
    num_comments AS current_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS score_velocity_per_minute,
    COALESCE(ROUND(a_score, 4), 0.0) AS score_acceleration,
    COALESCE(ROUND(a_norm, 6), 0.0) AS a_norm,
    COALESCE(ROUND(a_comment, 4), 0.0) AS comment_acceleration,
    CASE 
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 3600 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 60.0) AS VARCHAR), 'm')
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 86400 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 3600.0, 1) AS VARCHAR), 'h')
        ELSE CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 86400.0, 1) AS VARCHAR), 'd')
    END AS story_age,
    CONCAT(
        TIME_FORMAT("__time", 'HH:mm:ss', 'Asia/Kolkata'),
        ' (',
        CASE 
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 60 
                THEN 'just now'
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 3600 
                THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 60.0) AS VARCHAR), 'm ago')
            ELSE 
                CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 3600.0, 1) AS VARCHAR), 'h ago')
        END,
        ')'
    ) AS observation_time_ist
FROM "hn_snapshots"
WHERE latest_flag = 1 
  AND eligible = 1 
  AND a_score > 0
GROUP BY post_id, title, author, score, num_comments, v_score, a_score, a_norm, a_comment, created_at, url, "__time"
ORDER BY a_norm DESC
LIMIT 100;


-- ----------------------------------------------------------------------------
-- QUERY 8: Story Engagement Over Time (Full Trajectory Audit Table)
-- ----------------------------------------------------------------------------
-- Superset Viz: Table (Observation Time DESC, row limit 500)

SELECT
    CONCAT(
        TIME_FORMAT("__time", 'HH:mm:ss', 'Asia/Kolkata'),
        ' (',
        CASE 
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 60 
                THEN 'just now'
            WHEN (EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) < 3600 
                THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 60.0) AS VARCHAR), 'm ago')
            ELSE 
                CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM (SELECT MAX("__time") FROM "hn_snapshots")) - EXTRACT(EPOCH FROM "__time")) / 3600.0, 1) AS VARCHAR), 'h ago')
        END,
        ')'
    ) AS observation_time_ist,
    '<a href="' || 
    CASE 
        WHEN url IS NOT NULL AND url != '' THEN url 
        ELSE 'https://news.ycombinator.com/item?id=' || post_id 
    END || 
    '" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">' || 
    title || 
    '</a>' AS title_link,
    author,
    score,
    num_comments,
    COALESCE(ROUND(v_score, 2), 0.0) AS v_score,
    COALESCE(ROUND(a_score, 4), 0.0) AS a_score,
    CASE 
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 3600 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 60.0) AS VARCHAR), 'm')
        WHEN (EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) < 86400 
            THEN CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 3600.0, 1) AS VARCHAR), 'h')
        ELSE CONCAT(CAST(ROUND((EXTRACT(EPOCH FROM "__time") - EXTRACT(EPOCH FROM TIME_PARSE(created_at))) / 86400.0, 1) AS VARCHAR), 'd')
    END AS story_age,
    post_id,
    title,
    "__time"
FROM "hn_snapshots"
WHERE "__time" >= (SELECT MAX("__time") - INTERVAL '6' HOUR FROM "hn_snapshots")
GROUP BY post_id, title, author, score, num_comments, v_score, a_score, created_at, url, "__time"
ORDER BY "__time" DESC
LIMIT 500;
