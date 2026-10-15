#!/usr/bin/env python3
"""
Test Suite: InfoVelocity Druid SQL Queries for Velocity, Acceleration & Narrative Clusters
==========================================================================================
Executes Queries 1 through 8 against Apache Druid's SQL endpoint (port 8888).
Tests plain SELECT queries without window functions or subquery order bys.
"""

import os
import json
import requests

DRUID_URL = os.getenv("DRUID_ROUTER_URL", "http://localhost:8888") + "/druid/v2/sql"

QUERIES = [
    (
        "Query 1: Instantaneous Story Velocity",
        """
        SELECT 
            "__time",
            post_id,
            title,
            author,
            url,
            score,
            num_comments,
            dt_sec,
            v_score AS score_velocity_per_minute,
            v_comment AS comment_velocity_per_minute
        FROM "hn_snapshots"
        WHERE latest_flag = 1
          AND eligible = 1
          AND v_score IS NOT NULL
        GROUP BY "__time", post_id, title, author, url, score, num_comments, dt_sec, v_score, v_comment
        ORDER BY v_score DESC
        LIMIT 5
        """
    ),
    (
        "Query 2: Velocity Acceleration & Narrative Spurt Detection",
        """
        SELECT 
            "__time",
            post_id,
            title,
            author,
            url,
            score,
            num_comments,
            dt_sec,
            v_score AS score_velocity_per_minute,
            v_comment AS comment_velocity_per_minute,
            a_score AS score_acceleration,
            a_comment AS comment_acceleration,
            a_norm
        FROM "hn_snapshots"
        WHERE a_score IS NOT NULL
        GROUP BY "__time", post_id, title, author, url, score, num_comments, dt_sec, v_score, v_comment, a_score, a_comment, a_norm
        ORDER BY a_norm DESC
        LIMIT 5
        """
    ),
    (
        "Query 3: Real-Time Narrative Leaderboard",
        """
        SELECT
            post_id,
            title,
            author,
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
        GROUP BY post_id, title, author, score, num_comments, v_score, v_comment, a_score, a_comment, rank_score, "__time"
        ORDER BY rank_score DESC
        LIMIT 5
        """
    ),
    (
        "Query 4: Aggregate Engagement Velocity Trend (5-Min)",
        """
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
        ORDER BY bucket_time DESC
        LIMIT 5
        """
    ),
    (
        "Query 5: Top Accelerating 'Breakout' Stories",
        """
        SELECT
            post_id,
            title,
            author,
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
        GROUP BY post_id, title, author, score, num_comments, v_score, a_score, a_norm, a_comment, "__time"
        ORDER BY a_norm DESC
        LIMIT 5
        """
    ),
    (
        "Query 6: KPI Executive Summary Cards",
        """
        SELECT
            COUNT(DISTINCT post_id) AS active_stories,
            ROUND(AVG(CASE WHEN v_score IS NOT NULL THEN v_score END), 2) AS avg_score_velocity,
            ROUND(MAX(v_score), 2) AS highest_score_velocity,
            COALESCE(ROUND(MAX(CASE WHEN a_score > 0 THEN a_score END), 4), 0.0) AS highest_score_acceleration
        FROM "hn_snapshots"
        WHERE latest_flag = 1
          AND eligible = 1
        """
    ),
    (
        "Query 7: Narrative Clusters (Last 6h)",
        """
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
        LIMIT 5
        """
    ),
    (
        "Query 8: Story Engagement Over Time (Audit Table)",
        """
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
        LIMIT 5
        """
    )
]

def run_tests():
    print(f"Connecting to Druid SQL endpoint: {DRUID_URL}\n")
    all_passed = True
    for name, query in QUERIES:
        try:
            res = requests.post(DRUID_URL, json={"query": query.strip()}, timeout=10)
            if res.status_code == 200:
                data = res.json()
                print(f"=== {name} ===")
                print(f"Status: PASSED ({len(data)} rows returned)")
                if data:
                    print(json.dumps(data[0], indent=2))
                print()
            else:
                all_passed = False
                print(f"[FAILED] {name} (HTTP {res.status_code}): {res.text}")
        except Exception as e:
            all_passed = False
            print(f"[ERROR]  {name}: Could not connect to Druid ({e})")
    
    if all_passed:
        print("[SUCCESS] All 8 analytical SQL queries passed against Apache Druid!")
    else:
        print("[FAILED] One or more analytical queries failed.")
        exit(1)

if __name__ == "__main__":
    run_tests()
