#!/usr/bin/env python3
"""
InfoVelocity - Kinematics Processing Pipeline
=============================================
Computes instantaneous velocity (1st derivative) and acceleration (2nd derivative)
for each story across sequential time snapshots in Python before Druid ingestion.

Key Kinematics Rules:
  - 5-Minute Lookback Window:
      Compares current snapshot against the snapshot closest to 5 minutes (300s) earlier
      (falling back to the immediately preceding snapshot if no ~5m snapshot exists).
  - dt_sec: Real elapsed seconds between current snapshot and chosen lookback snapshot.
  - Min-dt & Gap Control:
      If dt_sec < 20s or dt_sec > 3 * POLL_INTERVAL (900s): v_score, v_comment = None.
  - v_score = max(0, dscore) / (dt_sec / 60.0)  [points / min]
  - v_comment = max(0, dcomments) / (dt_sec / 60.0) [comments / min] (rolling 5-min comment velocity)
  - Acceleration Guard:
      Acceleration is computed ONLY when:
        1. Both current and previous velocity windows have dt_sec >= 60s
        2. Score change (dscore) is at least 2 points
      This completely prevents small-dt jitter from saturating acceleration.
  - is_capped column:
      Flags any row capped by ACCELERATION_CAP_THRESHOLD (500.0 pts/min²) with is_capped = 1 (else 0).
  - a_norm = a_score / max(score, 1)
  - rank_score = v_score * ln(1 + score)
  - eligible = 1 if (score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS) else 0
  - latest_flag = 1 for the most recent observation of each story, 0 for historical observations.
"""

import os
import sys
import json
import math
import argparse
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

# Configurable constants
POLL_INTERVAL_DEFAULT = 300
MIN_DT_SEC = 20.0
MIN_ACCELERATION_DT_SEC = 20.0      # Both current and prior windows must have dt >= 20s
MIN_ACCELERATION_DSCORE = 1         # Require score change of at least 1 point to trigger acceleration
ACCELERATION_CAP_THRESHOLD = 500.0  # Safe upper bound far above real physical values
LOOKBACK_TARGET_SEC = 300.0         # 5 minutes lookback
MIN_ELIGIBLE_SCORE = 2              # Configurable eligibility threshold
MIN_ELIGIBLE_COMMENTS = 1           # Configurable eligibility threshold


def parse_timestamp_to_epoch(ts_str: str) -> float:
    """Parses ISO 8601 timestamp string to Unix epoch seconds."""
    if not ts_str:
        return 0.0
    try:
        clean_ts = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_ts)
        return dt.timestamp()
    except Exception:
        return 0.0


def process_snapshots_data(records: list[dict], poll_interval: int = POLL_INTERVAL_DEFAULT) -> list[dict]:
    """
    Groups records by post_id, sorts chronologically, and computes 5-minute lookback kinematics.
    Sets latest_flag = 1 on the latest observation of each story and 0 on older ones.
    Sets is_capped = 1 if a_score reaches the safety cap.
    Returns list of records with updated kinematic columns.
    """
    max_gap = 3 * poll_interval

    # Group by post_id
    by_post = defaultdict(list)
    for idx, rec in enumerate(records):
        post_id = str(rec.get("post_id", ""))
        ts_epoch = parse_timestamp_to_epoch(rec.get("timestamp"))
        by_post[post_id].append((ts_epoch, idx, rec))

    updated_records = [None] * len(records)

    for post_id, items in by_post.items():
        # Sort chronologically by timestamp
        items.sort(key=lambda x: x[0])
        total_items = len(items)

        # Track history for lookback comparisons:
        # [ { "epoch": ts_epoch, "score": score, "comments": comments, "dt_sec": dt_sec, "v_score": v_score, "v_comment": v_comment } ]
        history = []

        for i, (ts_epoch, orig_idx, snap) in enumerate(items):
            score = int(snap.get("score") or 0)
            comments = int(snap.get("num_comments") or snap.get("comments") or 0)

            # Eligibility filter: score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS
            eligible = 1 if (score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS) else 0

            # Latest flag: 1 for the latest observation of this post_id, 0 otherwise
            latest_flag = 1 if (i == total_items - 1) else 0

            dt_sec = None
            v_score = None
            v_comment = None
            a_score = None
            a_comment = None
            a_norm = None
            rank_score = None
            is_capped = 0

            if history and ts_epoch > 0:
                # Target lookback time: 5 minutes (300s) earlier
                target_epoch = ts_epoch - LOOKBACK_TARGET_SEC

                # Find candidate closest to target_epoch among prior snapshots
                best_prev = min(history, key=lambda h: abs(h["epoch"] - target_epoch))
                t_prev = best_prev["epoch"]
                score_prev = best_prev["score"]
                comment_prev = best_prev["comments"]
                dt_prev = best_prev.get("dt_sec")
                v_score_prev = best_prev.get("v_score")
                v_comment_prev = best_prev.get("v_comment")

                dt_val = ts_epoch - t_prev
                if dt_val >= 0:
                    dt_sec = round(dt_val, 2)

                # Velocity window validity
                if dt_sec is not None and MIN_DT_SEC <= dt_sec <= max_gap:
                    dt_min = dt_sec / 60.0
                    dscore = max(0, score - score_prev)
                    dcomment = max(0, comments - comment_prev)

                    v_score = round(dscore / dt_min, 4)
                    v_comment = round(dcomment / dt_min, 4)

                    # Fallback to closest valid prior snapshot if best_prev lacks velocity
                    if v_score_prev is None or dt_prev is None:
                        valid_prior = [h for h in history if h.get("v_score") is not None and h.get("dt_sec") is not None]
                        if valid_prior:
                            best_v_prev = min(valid_prior, key=lambda h: abs(h["epoch"] - (ts_epoch - MIN_ACCELERATION_DT_SEC)))
                            v_score_prev = best_v_prev.get("v_score")
                            v_comment_prev = best_v_prev.get("v_comment")
                            dt_prev = best_v_prev.get("dt_sec")
                            dt_acc_sec = max(ts_epoch - best_v_prev["epoch"], 1.0)
                        else:
                            dt_acc_sec = dt_sec
                    else:
                        dt_acc_sec = dt_sec

                    dt_acc_min = dt_acc_sec / 60.0

                    # Compute acceleration ONLY when:
                    # 1. Both current and previous windows have dt_sec >= MIN_ACCELERATION_DT_SEC
                    # 2. Score change is at least MIN_ACCELERATION_DSCORE
                    if (v_score_prev is not None and 
                        dt_sec >= MIN_ACCELERATION_DT_SEC and 
                        dt_prev is not None and dt_prev >= MIN_ACCELERATION_DT_SEC and 
                        dscore >= MIN_ACCELERATION_DSCORE):

                        raw_a = (v_score - v_score_prev) / dt_acc_min
                        if raw_a > ACCELERATION_CAP_THRESHOLD:
                            a_score = ACCELERATION_CAP_THRESHOLD
                            is_capped = 1
                        else:
                            a_score = round(raw_a, 6)
                            is_capped = 0
                        a_norm = round(a_score / max(score, 1), 6)
                    else:
                        a_score = None
                        a_norm = None
                        is_capped = 0

                    if (v_comment_prev is not None and 
                        dt_sec >= MIN_ACCELERATION_DT_SEC and 
                        dt_prev is not None and dt_prev >= MIN_ACCELERATION_DT_SEC and 
                        dcomment >= 1):
                        a_comment = round((v_comment - v_comment_prev) / dt_acc_min, 6)
                    else:
                        a_comment = None

                    rank_score = round(v_score * math.log(1.0 + max(score, 0)), 4)
                else:
                    v_score = None
                    v_comment = None
                    a_score = None
                    a_comment = None
                    a_norm = None
                    rank_score = None
                    is_capped = 0

            # Assign computed fields
            snap["dt_sec"] = dt_sec
            snap["v_score"] = v_score
            snap["v_comment"] = v_comment
            snap["a_score"] = a_score
            snap["a_comment"] = a_comment
            snap["a_norm"] = a_norm
            snap["rank_score"] = rank_score
            snap["eligible"] = eligible
            snap["latest_flag"] = latest_flag
            snap["is_capped"] = is_capped

            updated_records[orig_idx] = snap

            # Add to history
            history.append({
                "epoch": ts_epoch,
                "score": score,
                "comments": comments,
                "dt_sec": dt_sec,
                "v_score": v_score,
                "v_comment": v_comment
            })

    return [r for r in updated_records if r is not None]


def compute_file_kinematics(input_file: str, output_file: str = None, poll_interval: int = POLL_INTERVAL_DEFAULT) -> int:
    """Reads JSONL snapshots, computes kinematics with 5-minute lookback and latest_flag, and writes back."""
    if output_file is None:
        output_file = input_file

    in_path = Path(input_file)
    if not in_path.exists():
        print(f"[ERROR] Input file {input_file} does not exist.")
        return 0

    records = []
    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if not records:
        print(f"[WARN] No records found in {input_file}.")
        return 0

    processed = process_snapshots_data(records, poll_interval=poll_interval)

    out_path = Path(output_file)
    temp_path = out_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        for rec in processed:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    temp_path.replace(out_path)
    print(f"[SUCCESS] Computed kinematics for {len(processed)} snapshot records -> {output_file}")
    return len(processed)


class StoryKinematicsTracker:
    """
    In-memory tracker for live collector:
    Maintains historical observations per story to compute 5-minute lookback kinematics on the fly.
    """
    def __init__(self, poll_interval: int = POLL_INTERVAL_DEFAULT):
        self.poll_interval = poll_interval
        self.max_gap = 3 * poll_interval
        # post_id -> list of {"epoch": ts, "score": s, "comments": c, "dt_sec": dt, "v_score": v_s, "v_comment": v_c}
        self.history = defaultdict(list)

    def seed_from_file(self, snapshots_file: str):
        """Seeds state from existing snapshots file so restarts seamlessly resume."""
        p = Path(snapshots_file)
        if not p.exists():
            return
        try:
            records = []
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            records.sort(key=lambda r: parse_timestamp_to_epoch(r.get("timestamp")))
            for r in records:
                self.process_snapshot(r)
        except Exception as e:
            print(f"[WARN] Could not seed kinematics tracker: {e}")

    def process_snapshot(self, snap: dict) -> dict:
        """Computes kinematics for a single snapshot in-place using tracked 5-minute lookback."""
        post_id = str(snap.get("post_id", ""))
        ts_epoch = parse_timestamp_to_epoch(snap.get("timestamp"))
        score = int(snap.get("score") or 0)
        comments = int(snap.get("num_comments") or snap.get("comments") or 0)

        eligible = 1 if (score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS) else 0

        dt_sec = None
        v_score = None
        v_comment = None
        a_score = None
        a_comment = None
        a_norm = None
        rank_score = None
        is_capped = 0

        hist = self.history[post_id]
        if hist and ts_epoch > 0:
            target_epoch = ts_epoch - LOOKBACK_TARGET_SEC
            best_prev = min(hist, key=lambda h: abs(h["epoch"] - target_epoch))
            t_prev = best_prev["epoch"]
            score_prev = best_prev["score"]
            comment_prev = best_prev["comments"]
            dt_prev = best_prev.get("dt_sec")
            v_score_prev = best_prev.get("v_score")
            v_comment_prev = best_prev.get("v_comment")

            dt_val = ts_epoch - t_prev
            if dt_val >= 0:
                dt_sec = round(dt_val, 2)

            if dt_sec is not None and MIN_DT_SEC <= dt_sec <= self.max_gap:
                dt_min = dt_sec / 60.0
                dscore = max(0, score - score_prev)
                dcomment = max(0, comments - comment_prev)

                v_score = round(dscore / dt_min, 4)
                v_comment = round(dcomment / dt_min, 4)

                if (v_score_prev is not None and 
                    dt_sec >= MIN_ACCELERATION_DT_SEC and 
                    dt_prev is not None and dt_prev >= MIN_ACCELERATION_DT_SEC and 
                    dscore >= MIN_ACCELERATION_DSCORE):

                    raw_a = (v_score - v_score_prev) / dt_min
                    if raw_a > ACCELERATION_CAP_THRESHOLD:
                        a_score = ACCELERATION_CAP_THRESHOLD
                        is_capped = 1
                    else:
                        a_score = round(raw_a, 6)
                        is_capped = 0
                    a_norm = round(a_score / max(score, 1), 6)

                if (v_comment_prev is not None and 
                    dt_sec >= MIN_ACCELERATION_DT_SEC and 
                    dt_prev is not None and dt_prev >= MIN_ACCELERATION_DT_SEC and 
                    dcomment >= 1):
                    a_comment = round((v_comment - v_comment_prev) / dt_min, 6)

                rank_score = round(v_score * math.log(1.0 + max(score, 0)), 4)

        snap["dt_sec"] = dt_sec
        snap["v_score"] = v_score
        snap["v_comment"] = v_comment
        snap["a_score"] = a_score
        snap["a_comment"] = a_comment
        snap["a_norm"] = a_norm
        snap["rank_score"] = rank_score
        snap["eligible"] = eligible
        snap["latest_flag"] = 1
        snap["is_capped"] = is_capped

        # Save to history (keep last 30 observations per active story)
        hist.append({
            "epoch": ts_epoch,
            "score": score,
            "comments": comments,
            "dt_sec": dt_sec,
            "v_score": v_score,
            "v_comment": v_comment
        })
        if len(hist) > 30:
            hist.pop(0)

        return snap


def main():
    parser = argparse.ArgumentParser(description="Compute kinematics for Hacker News snapshots.")
    parser.add_argument("--input", default="hn_snapshots.jsonl", help="Input snapshots JSONL file.")
    parser.add_argument("--output", default=None, help="Output JSONL file (default: in-place overwrite).")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_DEFAULT, help="Polling interval in seconds.")
    args = parser.parse_args()

    compute_file_kinematics(args.input, args.output, poll_interval=args.interval)


if __name__ == "__main__":
    main()
