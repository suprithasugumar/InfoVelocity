#!/usr/bin/env python3
"""
InfoVelocity - Reproducible Demo Mode Generator
================================================
SYNTHETIC DATA ONLY: For presentation, validation, and offline demonstrations.

Generates deterministic, highly realistic synthetic Hacker News snapshot data
illustrating 5 distinct narrative dynamics across sequential time intervals:

1. Normal Steady Growth (Linear Velocity, Zero Acceleration)
2. Rapidly Accelerating Breakout (Spurt / Exponential Growth, High Positive Acceleration)
3. Decelerating Post (Saturating Topic, Negative Acceleration)
4. High-Debate / Controversial Story (Extreme Comment Velocity & Comment Acceleration)
5. Inactive / Flatline Post (Negligible Velocity)

Outputs:
- demo_hn_events.jsonl: First-seen catalog records
- demo_hn_snapshots.jsonl: Multi-observation time-series engagement records

Usage:
  python demo_data_generator.py
  python demo_data_generator.py --intervals 6 --step-minutes 5
"""

import os
import json
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path


# =============================================================================
# Narrative Archetypes & Ground-Truth Trajectories
# =============================================================================

DEMO_STORIES = [
    {
        "post_id": "90000001",
        "title": "Show HN: LightDB – SQLite-compatible in-memory database in Go",
        "author": "godev_sarah",
        "url": "https://github.com/example/lightdb",
        "archetype": "Steady Normal Growth",
        "created_offset_min": -30,
        # (score, comments) per 5-minute interval: steady +15 pts, +3 comments
        "trajectory": [
            (20, 4),    # t0 (18:30:00Z)
            (35, 7),    # t1 (18:35:00Z) -> v_score = 3.0 pts/min, v_comm = 0.6
            (50, 10),   # t2 (18:40:00Z) -> v_score = 3.0 pts/min, a_score = 0.0
            (65, 13),   # t3 (18:45:00Z) -> v_score = 3.0 pts/min, a_score = 0.0
            (80, 16),   # t4 (18:50:00Z) -> v_score = 3.0 pts/min, a_score = 0.0
            (95, 19),   # t5 (18:55:00Z) -> v_score = 3.0 pts/min, a_score = 0.0
        ]
    },
    {
        "post_id": "90000002",
        "title": "Critical 0-day vulnerability discovered in widespread SSH implementation",
        "author": "netsec_alice",
        "url": "https://cybersecurity.news/ssh-zero-day-2026",
        "archetype": "Rapidly Accelerating Breakout",
        "created_offset_min": -15,
        # Exploding growth: increasing velocity & positive acceleration
        "trajectory": [
            (5, 1),      # t0 (18:30:00Z)
            (15, 4),     # t1 (18:35:00Z) -> v = 2.0 pts/min
            (45, 16),    # t2 (18:40:00Z) -> v = 6.0 pts/min, a = +0.80 pts/min²
            (125, 56),   # t3 (18:45:00Z) -> v = 16.0 pts/min, a = +2.00 pts/min²
            (275, 136),  # t4 (18:50:00Z) -> v = 30.0 pts/min, a = +2.80 pts/min²
            (525, 266),  # t5 (18:55:00Z) -> v = 50.0 pts/min, a = +4.00 pts/min²
        ]
    },
    {
        "post_id": "90000003",
        "title": "Ask HN: How do you organize your home server backups?",
        "author": "sysadmin_bob",
        "url": "",  # Text post -> falls back to https://news.ycombinator.com/item?id=90000003
        "archetype": "Decelerating / Saturating Story",
        "created_offset_min": -60,
        # High initial counts, but decaying rate of growth (negative acceleration)
        "trajectory": [
            (180, 90),   # t0 (18:30:00Z)
            (230, 115),  # t1 (18:35:00Z) -> v = 10.0 pts/min
            (265, 132),  # t2 (18:40:00Z) -> v = 7.0 pts/min, a = -0.60 pts/min²
            (285, 142),  # t3 (18:45:00Z) -> v = 4.0 pts/min, a = -0.60 pts/min²
            (295, 147),  # t4 (18:50:00Z) -> v = 2.0 pts/min, a = -0.40 pts/min²
            (299, 149),  # t5 (18:55:00Z) -> v = 0.8 pts/min, a = -0.24 pts/min²
        ]
    },
    {
        "post_id": "90000004",
        "title": "Opinion: Why Python type hints do more harm than good in large codebases",
        "author": "code_contrarian",
        "url": "https://blog.devopinions.io/python-types-debate",
        "archetype": "Controversial / Comment Surge",
        "created_offset_min": -20,
        # Modest upvotes, but comments surge massively
        "trajectory": [
            (10, 8),     # t0 (18:30:00Z)
            (15, 28),    # t1 (18:35:00Z) -> comm_v = 4.0 comm/min
            (21, 68),    # t2 (18:40:00Z) -> comm_v = 8.0 comm/min, comm_a = +0.80
            (28, 138),   # t3 (18:45:00Z) -> comm_v = 14.0 comm/min, comm_a = +1.20
            (35, 238),   # t4 (18:50:00Z) -> comm_v = 20.0 comm/min, comm_a = +1.20
            (43, 368),   # t5 (18:55:00Z) -> comm_v = 26.0 comm/min, comm_a = +1.20
        ]
    },
    {
        "post_id": "90000005",
        "title": "Personal notes on configuring tmux on OpenBSD 7.5",
        "author": "bsd_user_99",
        "url": "https://bsd-notes.org/tmux-conf",
        "archetype": "Inactive / Flatline Story",
        "created_offset_min": -25,
        # Negligible growth
        "trajectory": [
            (1, 0),      # t0
            (2, 0),      # t1 -> v = 0.2 pts/min
            (2, 0),      # t2 -> v = 0.0 pts/min, a = -0.04
            (2, 0),      # t3 -> v = 0.0 pts/min, a = 0.00
            (3, 0),      # t4 -> v = 0.2 pts/min, a = +0.04
            (3, 0),      # t5 -> v = 0.0 pts/min, a = -0.04
        ]
    }
]


def generate_demo_dataset(base_time: datetime = None, step_minutes: int = 5, output_dir: str = "."):
    """
    Generates synthetic demo files conforming precisely to the InfoVelocity schema.
    """
    if base_time is None:
        # Fixed base timestamp for perfectly reproducible demonstration dates
        base_time = datetime(2026, 9, 17, 18, 30, 0, tzinfo=timezone.utc)

    events_file = Path(output_dir) / "demo_hn_events.jsonl"
    snapshots_file = Path(output_dir) / "demo_hn_snapshots.jsonl"
    spec_file = Path(output_dir) / "druid_demo_snapshots_ingestion_spec.json"

    events_records = []
    snapshots_records = []

    print(f"[*] Generating Reproducible Demo Dataset (Base time: {base_time.isoformat()})...")

    for story in DEMO_STORIES:
        created_at_dt = base_time + timedelta(minutes=story["created_offset_min"])
        created_at_iso = created_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        initial_score, initial_comments = story["trajectory"][0]

        # 1. First-seen event record
        event_record = {
            "post_id": story["post_id"],
            "timestamp": created_at_iso,
            "title": story["title"],
            "body": "",
            "author": story["author"],
            "source": "hackernews",
            "subreddit": "hackernews",
            "url": story["url"],
            "score": initial_score,
            "num_comments": initial_comments
        }
        events_records.append(event_record)

        # 2. Sequential snapshot observations
        for i, (score, comments) in enumerate(story["trajectory"]):
            obs_dt = base_time + timedelta(minutes=i * step_minutes)
            obs_iso = obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            snapshot_record = {
                "post_id": story["post_id"],
                "timestamp": obs_iso,
                "created_at": created_at_iso,
                "title": story["title"],
                "author": story["author"],
                "source": "hackernews",
                "subreddit": "hackernews",
                "url": story["url"],
                "score": score,
                "num_comments": comments
            }
            snapshots_records.append(snapshot_record)

    # Write events JSONL
    with open(events_file, "w", encoding="utf-8") as f:
        for r in events_records:
            f.write(json.dumps(r) + "\n")
    print(f"[SUCCESS] Wrote {len(events_records)} catalog records to: {events_file}")

    # Write snapshots JSONL
    with open(snapshots_file, "w", encoding="utf-8") as f:
        for r in snapshots_records:
            f.write(json.dumps(r) + "\n")
    print(f"[SUCCESS] Wrote {len(snapshots_records)} snapshot observations to: {snapshots_file}")

    # Create dedicated Druid Demo Spec
    demo_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": "demo_hn_snapshots",
                "timestampSpec": {
                    "column": "timestamp",
                    "format": "iso"
                },
                "dimensionsSpec": {
                    "dimensions": [
                        "post_id",
                        "author",
                        "source",
                        "subreddit",
                        "title",
                        "url",
                        "created_at"
                    ]
                },
                "metricsSpec": [
                    {
                        "type": "count",
                        "name": "count"
                    },
                    {
                        "type": "longSum",
                        "name": "score",
                        "fieldName": "score"
                    },
                    {
                        "type": "longSum",
                        "name": "num_comments",
                        "fieldName": "num_comments"
                    }
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "HOUR",
                    "queryGranularity": "NONE",
                    "rollup": False
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "local",
                    "baseDir": "/opt/shared",
                    "filter": "demo_hn_snapshots.jsonl"
                },
                "inputFormat": {
                    "type": "json"
                },
                "appendToExisting": False
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsInMemory": 100000,
                "maxRowsPerSegment": 5000000
            }
        }
    }

    with open(spec_file, "w", encoding="utf-8") as f:
        json.dump(demo_spec, f, indent=2)
    print(f"[SUCCESS] Wrote Druid demo ingestion spec to: {spec_file}")

    # Print mathematical kinematics table
    print_mathematical_validation()


def print_mathematical_validation():
    """
    Computes and displays the exact kinematics for the demo dataset to demonstrate
    velocity and acceleration behavior clearly during presentations.
    """
    print("\n" + "=" * 105)
    print("DEMO MODE: MATHEMATICAL KINEMATICS & DERIVATIVE VERIFICATION")
    print("=" * 105)
    print(f"{'Story ID':<10} | {'Archetype':<32} | {'Interval':<12} | {'Score':<6} | {'v_score (pts/min)':<18} | {'a_score (pts/min^2)':<20}")
    print("-" * 110)

    for story in DEMO_STORIES:
        pid = story["post_id"]
        arch = story["archetype"]
        traj = story["trajectory"]
        velocities = []

        for i in range(len(traj)):
            curr_score, curr_comm = traj[i]
            if i == 0:
                v_score = 0.0
                a_score = 0.0
                velocities.append(0.0)
                interval_label = "t0 (Base)"
            else:
                prev_score, _ = traj[i-1]
                v_score = (curr_score - prev_score) / 5.0
                velocities.append(v_score)
                if i == 1:
                    a_score = 0.0
                    interval_label = f"t{i} (+{i*5}m)"
                else:
                    prev_v = velocities[i-1]
                    a_score = (v_score - prev_v) / 5.0
                    interval_label = f"t{i} (+{i*5}m)"

            print(f"{pid:<10} | {arch:<32} | {interval_label:<12} | {curr_score:<6} | {v_score:>10.2f} pts/min | {a_score:>12.2f} pts/min^2")
        print("-" * 110)

    print("\n[KEY TAKEAWAY FOR DEMO]:")
    print("* Story 90000002 (0-day Exploit) exhibits continuous velocity increase (2.0 -> 50.0 pts/min)")
    print("  and high positive acceleration (+4.00 pts/min^2), dominating the Breakout Monitor.")
    print("* Story 90000003 (Backups) decelerates from 10.0 -> 0.8 pts/min with negative acceleration (-0.60 pts/min^2).")
    print("* Story 90000004 (Python Types) surges in comment velocity (4.0 -> 26.0 comments/min).")
    print("=" * 110 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic reproducible demo data for InfoVelocity.")
    parser.add_argument("--step-minutes", type=int, default=5, help="Step minutes between snapshot intervals")
    parser.add_argument("--output-dir", default=".", help="Output directory for generated files")
    args = parser.parse_args()

    generate_demo_dataset(step_minutes=args.step_minutes, output_dir=args.output_dir)
