#!/usr/bin/env python3
"""
InfoVelocity - Kinematics Processing & Narrative Clustering Pipeline
====================================================================
Computes 5-minute velocity, 10-minute acceleration, and 30-minute velocity:
  - VELOCITY_WINDOW_MIN = 5 (W = 300s):
      v_score(t) = (score(t) - score(t - W)) / W_actual
      Reference snapshot nearest to t - W (tolerance +/- 90s). Same for v_comment.
  - ACCEL_WINDOW_MIN = 10 (A = 600s):
      a_score(t) = (v_A(t) - v_A(t - A)) / A_actual
      Needs 2A (20 min) of history, otherwise null. Same for comment acceleration.
  - v_score_30m: 30-minute velocity as extra context.
  - a_norm = a_score / max(score, 1)
  - rank_score = v_score * ln(1 + score)
  - If no reference snapshot exists within tolerance, or gap > 3x poll interval: null (never 0).
  - Precomputes display dimensions (title_link, story_age, observation_time_ist, quadrant).
  - Narrative clusters generated into dedicated hn_clusters.jsonl (min 2 stories, title similarity).
"""

import os
import sys
import json
import math
import re
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict, Counter

# Configurable constants
POLL_INTERVAL_DEFAULT = 60
VELOCITY_WINDOW_MIN = 5
ACCEL_WINDOW_MIN = 10
VELOCITY_30M_WINDOW_MIN = 30

VELOCITY_WINDOW_SEC = VELOCITY_WINDOW_MIN * 60.0       # 300.0s
ACCEL_WINDOW_SEC = ACCEL_WINDOW_MIN * 60.0             # 600.0s
VELOCITY_30M_WINDOW_SEC = VELOCITY_30M_WINDOW_MIN * 60.0 # 1800.0s

TOLERANCE_SEC = 90.0            # Tolerance for 5m and 10m window matching (+/- 1.5 intervals)
TOLERANCE_30M_SEC = 180.0       # Tolerance for 30m window matching
MIN_DT_SEC = 20.0
ACCELERATION_CAP_THRESHOLD = 500.0
MIN_ELIGIBLE_SCORE = 2
MIN_ELIGIBLE_COMMENTS = 1

STOPWORDS = {
    "the", "a", "an", "and", "or", "in", "on", "at", "to", "for", "of", "with",
    "by", "from", "up", "about", "into", "over", "after", "is", "are", "was",
    "were", "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "but", "if", "then", "else", "when", "where", "why", "how", "all", "any",
    "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
    "not", "only", "own", "same", "so", "than", "too", "very", "can", "will",
    "just", "should", "now", "show", "hn", "ask", "tell", "what", "which", "your",
    "you", "our", "new", "this", "that", "these", "those"
}


def fix_mojibake(text: str) -> str:
    """Fix double-encoded UTF-8 mojibake tokens into clean Unicode."""
    if not text:
        return ""
    replacements = {
        'â€”': '—',
        'â€“': '–',
        'Â·': '·',
        'â€™': '’',
        'â€˜': '‘',
        'â€œ': '“',
        'â€\x9d': '”',
        'â€': '”',
        'Ã©': 'é',
        'â€¦': '…',
        'Â': '',
        '\ufffd': '—'
    }
    for bad, good in replacements.items():
        if bad:
            text = text.replace(bad, good)
    return text.strip()


def extract_tokens(title: str) -> set[str]:
    """Extract normalized alphanumeric words from title, removing stopwords."""
    if not title:
        return set()
    clean_title = fix_mojibake(title)
    words = re.findall(r'[a-zA-Z0-9\-\+\#]{3,}', clean_title.lower())
    return {w for w in words if w not in STOPWORDS}


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


def check_outage_gap(history_slice: list[dict], max_gap_sec: float) -> bool:
    """Returns True if any gap between consecutive snapshots in history_slice exceeds max_gap_sec."""
    for idx in range(1, len(history_slice)):
        if (history_slice[idx]["epoch"] - history_slice[idx - 1]["epoch"]) > max_gap_sec:
            return True
    return False


def process_snapshots_data(records: list[dict], poll_interval: int = POLL_INTERVAL_DEFAULT) -> list[dict]:
    """
    Groups records by post_id, sorts chronologically, and computes:
    - 5-min velocity: v_score, v_comment
    - 30-min velocity: v_score_30m
    - 10-min acceleration: a_score, a_comment (requires 20 min history)
    - Normalized metrics: a_norm, rank_score
    - Latest flag & eligibility
    - Display dimensions: title_link, story_age, observation_time_ist, quadrant
    """
    max_gap = 3.0 * poll_interval  # Gap threshold for outage detection (e.g. 180s)
    now_epoch = datetime.now(timezone.utc).timestamp()

    tolerance_sec = float(poll_interval)
    tolerance_30m_sec = 2.0 * float(poll_interval)

    by_post = defaultdict(list)
    for idx, rec in enumerate(records):
        post_id = str(rec.get("post_id", ""))
        ts_epoch = parse_timestamp_to_epoch(rec.get("timestamp"))
        by_post[post_id].append((ts_epoch, idx, rec))

    updated_records = [None] * len(records)

    for post_id, items in by_post.items():
        items.sort(key=lambda x: x[0])
        total_items = len(items)
        history = []

        for i, (ts_epoch, orig_idx, snap) in enumerate(items):
            score = int(snap.get("score") or 0)
            comments = int(snap.get("num_comments") or snap.get("comments") or 0)

            eligible = 1 if (score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS) else 0
            latest_flag = 1 if (i == total_items - 1) else 0

            dt_sec = None
            v_score = None
            v_comment = None
            v_score_30m = None
            a_score = None
            a_comment = None
            a_norm = None
            rank_score = None
            is_capped = 0

            if history and ts_epoch > 0:
                # -------------------------------------------------------------
                # 1. 5-Minute Velocity: v_score, v_comment
                # -------------------------------------------------------------
                # Nearest snapshot to t - 300s within tolerance (+/- poll_interval)
                v5_candidates = [
                    h for h in history 
                    if abs((ts_epoch - h["epoch"]) - VELOCITY_WINDOW_SEC) <= tolerance_sec
                ]
                if v5_candidates:
                    ref_5m = min(v5_candidates, key=lambda h: abs((ts_epoch - h["epoch"]) - VELOCITY_WINDOW_SEC))
                    dt_val = ts_epoch - ref_5m["epoch"]
                    if dt_val >= MIN_DT_SEC:
                        # Check for collector outage gap in window
                        sub_hist = [h for h in history if ref_5m["epoch"] <= h["epoch"] <= ts_epoch] + [{"epoch": ts_epoch}]
                        if not check_outage_gap(sub_hist, max_gap):
                            dt_sec = round(dt_val, 2)
                            dt_min = dt_val / 60.0
                            dscore = score - ref_5m["score"]
                            dcomment = comments - ref_5m["comments"]
                            v_score = round(dscore / dt_min, 4)
                            v_comment = round(dcomment / dt_min, 4)
                            rank_score = round(v_score * math.log(1.0 + max(score, 0)), 4)

                # -------------------------------------------------------------
                # 2. 30-Minute Velocity: v_score_30m
                # -------------------------------------------------------------
                v30_candidates = [
                    h for h in history 
                    if abs((ts_epoch - h["epoch"]) - VELOCITY_30M_WINDOW_SEC) <= tolerance_30m_sec
                ]
                if v30_candidates:
                    ref_30m = min(v30_candidates, key=lambda h: abs((ts_epoch - h["epoch"]) - VELOCITY_30M_WINDOW_SEC))
                    dt_30m_val = ts_epoch - ref_30m["epoch"]
                    if dt_30m_val >= MIN_DT_SEC:
                        dt_30m_min = dt_30m_val / 60.0
                        dscore_30m = score - ref_30m["score"]
                        v_score_30m = round(dscore_30m / dt_30m_min, 4)

                # -------------------------------------------------------------
                # 3. 10-Minute Acceleration: a_score, a_comment
                # -------------------------------------------------------------
                # a_score(t) = (v_A(t) - v_A(t - A)) / A with A = 10 min.
                # Needs 2A (20 min) of history, otherwise null.
                earliest_epoch = history[0]["epoch"]
                if (ts_epoch - earliest_epoch) >= (2.0 * ACCEL_WINDOW_SEC - tolerance_sec):
                    # Find ref1 around t - 10m (600s)
                    cand_10m_1 = [
                        h for h in history 
                        if abs((ts_epoch - h["epoch"]) - ACCEL_WINDOW_SEC) <= tolerance_sec
                    ]
                    if cand_10m_1:
                        ref1 = min(cand_10m_1, key=lambda h: abs((ts_epoch - h["epoch"]) - ACCEL_WINDOW_SEC))
                        t1 = ref1["epoch"]
                        # Find ref2 around t1 - 10m (600s) -> total ~20m
                        cand_10m_2 = [
                            h for h in history 
                            if abs((t1 - h["epoch"]) - ACCEL_WINDOW_SEC) <= tolerance_sec
                        ]
                        if cand_10m_2:
                            ref2 = min(cand_10m_2, key=lambda h: abs((t1 - h["epoch"]) - ACCEL_WINDOW_SEC))
                            dt1_val = ts_epoch - t1
                            dt2_val = t1 - ref2["epoch"]
                            if dt1_val >= MIN_DT_SEC and dt2_val >= MIN_DT_SEC:
                                sub_hist_acc = [h for h in history if ref2["epoch"] <= h["epoch"] <= ts_epoch] + [{"epoch": ts_epoch}]
                                if not check_outage_gap(sub_hist_acc, max_gap):
                                    dt1_min = dt1_val / 60.0
                                    dt2_min = dt2_val / 60.0
                                    v_A_now = (score - ref1["score"]) / dt1_min
                                    v_A_prev = (ref1["score"] - ref2["score"]) / dt2_min
                                    raw_a = (v_A_now - v_A_prev) / dt1_min

                                    v_comm_A_now = (comments - ref1["comments"]) / dt1_min
                                    v_comm_A_prev = (ref1["comments"] - ref2["comments"]) / dt2_min
                                    raw_a_comm = (v_comm_A_now - v_comm_A_prev) / dt1_min

                                    if raw_a > ACCELERATION_CAP_THRESHOLD:
                                        a_score = ACCELERATION_CAP_THRESHOLD
                                        is_capped = 1
                                    elif raw_a < -ACCELERATION_CAP_THRESHOLD:
                                        a_score = -ACCELERATION_CAP_THRESHOLD
                                        is_capped = 1
                                    else:
                                        a_score = round(raw_a, 6)
                                        is_capped = 0
                                    a_norm = round(a_score / max(score, 1), 6)
                                    a_comment = round(raw_a_comm, 6)

            # Precomputed display dimensions
            raw_title = str(snap.get("title") or "").strip()
            clean_title = fix_mojibake(raw_title)
            snap["title"] = clean_title

            url = str(snap.get("url") or "").strip()
            link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
            title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{clean_title}</a>'

            created_epoch = parse_timestamp_to_epoch(snap.get("created_at"))
            # Story Age = now - created_at (or ts_epoch - created_epoch)
            diff_sec = max(0, (now_epoch if latest_flag == 1 else ts_epoch) - created_epoch) if created_epoch > 0 else 0
            if diff_sec < 3600:
                story_age = f"{int(round(diff_sec / 60.0))}m"
            elif diff_sec < 86400:
                story_age = f"{round(diff_sec / 3600.0, 1)}h"
            else:
                story_age = f"{round(diff_sec / 86400.0, 1)}d"

            if ts_epoch > 0:
                dt_ist = datetime.fromtimestamp(ts_epoch, tz=timezone.utc) + timedelta(hours=5, minutes=30)
                observation_time_ist = dt_ist.strftime("%H:%M:%S")
            else:
                observation_time_ist = ""

            if v_score is not None and a_score is not None:
                if v_score > 0 and a_score > 0:
                    quadrant = "Breakout (Accelerating)"
                elif v_score > 0 and a_score < 0:
                    quadrant = "Cooling (Slowing)"
                elif v_score > 0 and (a_score == 0 or a_score is None):
                    quadrant = "Steady (Linear Growth)"
                else:
                    quadrant = "Dormant / Static"
            elif v_score is not None and v_score > 0:
                quadrant = "Steady (Linear Growth)"
            else:
                quadrant = "Dormant / Static"

            # Assign computed fields
            snap["dt_sec"] = dt_sec
            snap["v_score"] = v_score
            snap["v_comment"] = v_comment
            snap["v_score_30m"] = v_score_30m
            snap["a_score"] = a_score
            snap["a_comment"] = a_comment
            snap["a_norm"] = a_norm
            snap["rank_score"] = rank_score
            snap["eligible"] = eligible
            snap["latest_flag"] = latest_flag
            snap["is_capped"] = is_capped
            snap["title_link"] = title_link
            snap["story_age"] = story_age
            snap["observation_time_ist"] = observation_time_ist
            snap["quadrant"] = quadrant

            # Never write cluster columns into story snapshot rows
            snap.pop("cluster_id", None)
            snap.pop("cluster_label", None)

            updated_records[orig_idx] = snap

            history.append({
                "epoch": ts_epoch,
                "score": score,
                "comments": comments
            })

    return [r for r in updated_records if r is not None]


def generate_clusters_dataset(snapshots: list[dict], min_stories: int = 2, output_file: str = "hn_clusters.jsonl") -> list[dict]:
    """
    Generates a dedicated narrative clusters dataset:
    - Filters for the latest observation per post_id.
    - Groups stories by title similarity first (shared key phrases & semantic topics).
    - Strictly enforces min 2 stories per cluster (never singletons).
    - Writes records to hn_clusters.jsonl without polluting story rows.
    - If no clusters found, writes empty file.
    """
    latest_stories = {}
    for r in snapshots:
        if r.get("latest_flag") == 1:
            pid = str(r["post_id"])
            latest_stories[pid] = r

    if not latest_stories:
        by_post = defaultdict(list)
        for r in snapshots:
            by_post[str(r["post_id"])].append(r)
        for pid, lst in by_post.items():
            lst.sort(key=lambda x: parse_timestamp_to_epoch(x.get("timestamp")))
            latest_stories[pid] = lst[-1]

    # Pre-defined semantic title themes to aggregate closely related title phrases
    thematic_groups = [
        ("cluster_ai_llm", "AI, LLMs & Agent Architectures", {"ai", "llm", "gpt", "agent", "agents", "model", "models", "prompt", "neural", "deepseek", "claude"}),
        ("cluster_security", "Cybersecurity & Vulnerability Exploits", {"security", "vulnerability", "vulnerabilities", "exploit", "cve", "breach", "malware", "auth", "authentication", "password"}),
        ("cluster_os_systems", "Systems, Kernels & Hardware Architecture", {"linux", "kernel", "os", "hardware", "cpu", "gpu", "chip", "mac", "macos", "rust", "c++", "memory"}),
        ("cluster_web_dev", "Web Platforms, DevTools & Browsers", {"browser", "web", "html", "css", "sqlite", "database", "api", "framework", "tool", "tools", "extension"}),
        ("cluster_business_tech", "Tech Industry, Economy & Science", {"market", "economy", "business", "tax", "court", "law", "supermarket", "game", "book", "history"})
    ]

    token_to_pids = defaultdict(set)
    for pid, s in latest_stories.items():
        tokens = extract_tokens(s.get("title", ""))
        for t in tokens:
            token_to_pids[t].add(pid)

    assigned_pids = set()
    cluster_objects = []

    # 1. Thematic groups
    for cid, cname, kw_set in thematic_groups:
        matched = []
        for pid, s in latest_stories.items():
            if pid in assigned_pids:
                continue
            s_tokens = extract_tokens(s.get("title", ""))
            if s_tokens & kw_set:
                matched.append(s)

        if len(matched) >= min_stories:
            for s in matched:
                assigned_pids.add(str(s["post_id"]))
            cluster_objects.append({
                "cluster_id": cid,
                "cluster_name": cname,
                "stories": matched
            })

    # 2. Dynamic title keyword overlap
    keyword_freq = {t: len(pids - assigned_pids) for t, pids in token_to_pids.items()}
    sorted_keywords = sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True)

    for kw, cnt in sorted_keywords:
        if cnt < min_stories:
            break
        candidates = [latest_stories[pid] for pid in token_to_pids[kw] if pid not in assigned_pids]
        if len(candidates) >= min_stories:
            for s in candidates:
                assigned_pids.add(str(s["post_id"]))
            cluster_objects.append({
                "cluster_id": f"cluster_{kw}",
                "cluster_name": f"{kw.capitalize()} Discussions",
                "stories": candidates
            })

    latest_ts = max(
        (r.get("timestamp") for r in snapshots if r.get("timestamp")), 
        default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    cluster_records = []
    for c in cluster_objects:
        st_list = c["stories"]
        tot_score = sum(int(s.get("score") or 0) for s in st_list)
        tot_vel = round(sum(float(s.get("v_score") or 0.0) for s in st_list), 2)
        top_story = max(st_list, key=lambda s: int(s.get("score") or 0))
        top_title = fix_mojibake(top_story.get("title", ""))

        cluster_records.append({
            "timestamp": latest_ts,
            "cluster_id": c["cluster_id"],
            "cluster": c["cluster_name"],
            "number_of_stories": len(st_list),
            "combined_score": tot_score,
            "combined_velocity": tot_vel,
            "top_title": top_title,
            "latest_activity_time": latest_ts
        })

    if output_file:
        out_path = Path(output_file)
        with open(out_path, "w", encoding="utf-8") as f:
            for rec in cluster_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[SUCCESS] Generated {len(cluster_records)} narrative clusters (min 2 stories) -> {output_file}")

    return cluster_records


def compute_file_kinematics(input_file: str, output_file: str = None, poll_interval: int = POLL_INTERVAL_DEFAULT) -> int:
    """Reads JSONL snapshots, computes kinematics, writes back snapshots and updates hn_clusters.jsonl."""
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

    # Generate separate hn_clusters dataset
    generate_clusters_dataset(processed, min_stories=2, output_file="hn_clusters.jsonl")

    return len(processed)


class StoryKinematicsTracker:
    """
    In-memory tracker for live collector:
    Maintains historical observations per story to compute kinematics on the fly.
    """
    def __init__(self, poll_interval: int = POLL_INTERVAL_DEFAULT):
        self.poll_interval = poll_interval
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
        """Computes kinematics for a single snapshot in-place using tracked lookback."""
        post_id = str(snap.get("post_id", ""))
        ts_epoch = parse_timestamp_to_epoch(snap.get("timestamp"))
        score = int(snap.get("score") or 0)
        comments = int(snap.get("num_comments") or snap.get("comments") or 0)

        eligible = 1 if (score >= MIN_ELIGIBLE_SCORE or comments >= MIN_ELIGIBLE_COMMENTS) else 0

        dt_sec = None
        v_score = None
        v_comment = None
        v_score_30m = None
        a_score = None
        a_comment = None
        a_norm = None
        rank_score = None
        is_capped = 0

        hist = self.history[post_id]
        tolerance_sec = float(self.poll_interval)
        tolerance_30m_sec = 2.0 * float(self.poll_interval)
        max_gap = 3.0 * float(self.poll_interval)

        if hist and ts_epoch > 0:
            # 5m velocity
            v5_candidates = [
                h for h in hist 
                if abs((ts_epoch - h["epoch"]) - VELOCITY_WINDOW_SEC) <= tolerance_sec
            ]
            if v5_candidates:
                ref_5m = min(v5_candidates, key=lambda h: abs((ts_epoch - h["epoch"]) - VELOCITY_WINDOW_SEC))
                dt_val = ts_epoch - ref_5m["epoch"]
                if dt_val >= MIN_DT_SEC:
                    sub_hist = [h for h in hist if ref_5m["epoch"] <= h["epoch"] <= ts_epoch] + [{"epoch": ts_epoch}]
                    if not check_outage_gap(sub_hist, max_gap):
                        dt_sec = round(dt_val, 2)
                        dt_min = dt_val / 60.0
                        dscore = score - ref_5m["score"]
                        dcomment = comments - ref_5m["comments"]
                        v_score = round(dscore / dt_min, 4)
                        v_comment = round(dcomment / dt_min, 4)
                        rank_score = round(v_score * math.log(1.0 + max(score, 0)), 4)

            # 30m velocity
            v30_candidates = [
                h for h in hist 
                if abs((ts_epoch - h["epoch"]) - VELOCITY_30M_WINDOW_SEC) <= tolerance_30m_sec
            ]
            if v30_candidates:
                ref_30m = min(v30_candidates, key=lambda h: abs((ts_epoch - h["epoch"]) - VELOCITY_30M_WINDOW_SEC))
                dt_30m_val = ts_epoch - ref_30m["epoch"]
                if dt_30m_val >= MIN_DT_SEC:
                    dt_30m_min = dt_30m_val / 60.0
                    dscore_30m = score - ref_30m["score"]
                    v_score_30m = round(dscore_30m / dt_30m_min, 4)

            # 10m acceleration (requires 20m history)
            earliest_epoch = hist[0]["epoch"]
            if (ts_epoch - earliest_epoch) >= (2.0 * ACCEL_WINDOW_SEC - tolerance_sec):
                cand_10m_1 = [
                    h for h in hist 
                    if abs((ts_epoch - h["epoch"]) - ACCEL_WINDOW_SEC) <= tolerance_sec
                ]
                if cand_10m_1:
                    ref1 = min(cand_10m_1, key=lambda h: abs((ts_epoch - h["epoch"]) - ACCEL_WINDOW_SEC))
                    t1 = ref1["epoch"]
                    cand_10m_2 = [
                        h for h in hist 
                        if abs((t1 - h["epoch"]) - ACCEL_WINDOW_SEC) <= tolerance_sec
                    ]
                    if cand_10m_2:
                        ref2 = min(cand_10m_2, key=lambda h: abs((t1 - h["epoch"]) - ACCEL_WINDOW_SEC))
                        dt1_val = ts_epoch - t1
                        dt2_val = t1 - ref2["epoch"]
                        if dt1_val >= MIN_DT_SEC and dt2_val >= MIN_DT_SEC:
                            sub_hist_acc = [h for h in hist if ref2["epoch"] <= h["epoch"] <= ts_epoch] + [{"epoch": ts_epoch}]
                            if not check_outage_gap(sub_hist_acc, max_gap):
                                dt1_min = dt1_val / 60.0
                                dt2_min = dt2_val / 60.0
                                v_A_now = (score - ref1["score"]) / dt1_min
                                v_A_prev = (ref1["score"] - ref2["score"]) / dt2_min
                                raw_a = (v_A_now - v_A_prev) / dt1_min

                                v_comm_A_now = (comments - ref1["comments"]) / dt1_min
                                v_comm_A_prev = (ref1["comments"] - ref2["comments"]) / dt2_min
                                raw_a_comm = (v_comm_A_now - v_comm_A_prev) / dt1_min

                                if raw_a > ACCELERATION_CAP_THRESHOLD:
                                    a_score = ACCELERATION_CAP_THRESHOLD
                                    is_capped = 1
                                elif raw_a < -ACCELERATION_CAP_THRESHOLD:
                                    a_score = -ACCELERATION_CAP_THRESHOLD
                                    is_capped = 1
                                else:
                                    a_score = round(raw_a, 6)
                                    is_capped = 0
                                a_norm = round(a_score / max(score, 1), 6)
                                a_comment = round(raw_a_comm, 6)

        # Precomputed display dimensions
        raw_title = str(snap.get("title") or "").strip()
        clean_title = fix_mojibake(raw_title)
        snap["title"] = clean_title

        url = str(snap.get("url") or "").strip()
        link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
        title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{clean_title}</a>'

        now_epoch = datetime.now(timezone.utc).timestamp()
        created_epoch = parse_timestamp_to_epoch(snap.get("created_at"))
        diff_sec = max(0, now_epoch - created_epoch) if created_epoch > 0 else 0
        if diff_sec < 3600:
            story_age = f"{int(round(diff_sec / 60.0))}m"
        elif diff_sec < 86400:
            story_age = f"{round(diff_sec / 3600.0, 1)}h"
        else:
            story_age = f"{round(diff_sec / 86400.0, 1)}d"

        if ts_epoch > 0:
            dt_ist = datetime.fromtimestamp(ts_epoch, tz=timezone.utc) + timedelta(hours=5, minutes=30)
            observation_time_ist = dt_ist.strftime("%H:%M:%S")
        else:
            observation_time_ist = ""

        if v_score is not None and a_score is not None:
            if v_score > 0 and a_score > 0:
                quadrant = "Breakout (Accelerating)"
            elif v_score > 0 and a_score < 0:
                quadrant = "Cooling (Slowing)"
            elif v_score > 0 and (a_score == 0 or a_score is None):
                quadrant = "Steady (Linear Growth)"
            else:
                quadrant = "Dormant / Static"
        elif v_score is not None and v_score > 0:
            quadrant = "Steady (Linear Growth)"
        else:
            quadrant = "Dormant / Static"

        snap["dt_sec"] = dt_sec
        snap["v_score"] = v_score
        snap["v_comment"] = v_comment
        snap["v_score_30m"] = v_score_30m
        snap["a_score"] = a_score
        snap["a_comment"] = a_comment
        snap["a_norm"] = a_norm
        snap["rank_score"] = rank_score
        snap["eligible"] = eligible
        snap["latest_flag"] = 1
        snap["is_capped"] = is_capped
        snap["title_link"] = title_link
        snap["story_age"] = story_age
        snap["observation_time_ist"] = observation_time_ist
        snap["quadrant"] = quadrant

        snap.pop("cluster_id", None)
        snap.pop("cluster_label", None)

        hist.append({
            "epoch": ts_epoch,
            "score": score,
            "comments": comments
        })

        return snap


def main():
    parser = argparse.ArgumentParser(description="InfoVelocity Kinematics Pre-computation")
    parser.add_argument("--input", default="hn_snapshots.jsonl", help="Input snapshots JSONL file")
    parser.add_argument("--output", default=None, help="Output JSONL file (default: overwrite input)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_DEFAULT, help="Poll interval in seconds")
    args = parser.parse_args()

    count = compute_file_kinematics(args.input, args.output, poll_interval=args.interval)
    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
