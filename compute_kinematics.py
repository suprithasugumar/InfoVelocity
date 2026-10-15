#!/usr/bin/env python3
"""
InfoVelocity - Kinematics Processing & Narrative Clustering Pipeline
====================================================================
Computes instantaneous velocity (1st derivative) and acceleration (2nd derivative)
for each story across sequential time snapshots in Python before Druid ingestion.
Precomputes display columns (title_link, story_age, observation_time_ist, quadrant).

Separation of Concerns:
- Story rows in hn_snapshots contain ONLY story metrics and kinematics.
- Narrative clusters are generated into a separate dataset (hn_clusters.jsonl)
  with min 2 stories per cluster, grouped by title similarity first.
- Strict UTF-8 encoding end-to-end to eliminate mojibake.
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
MIN_DT_SEC = 20.0
MIN_ACCELERATION_DT_SEC = 20.0
MIN_ACCELERATION_DSCORE = 1
ACCELERATION_CAP_THRESHOLD = 500.0
LOOKBACK_TARGET_SEC = 300.0
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
        'Â': ''
    }
    for bad, good in replacements.items():
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


def process_snapshots_data(records: list[dict], poll_interval: int = POLL_INTERVAL_DEFAULT) -> list[dict]:
    """
    Groups records by post_id, sorts chronologically, and computes:
    - Kinematics (v_score, v_comment, a_score, a_comment, rank_score, a_norm)
    - Latest flag & eligibility
    - Precomputed display dimensions (title_link, story_age, observation_time_ist, quadrant)
    NOTE: Never writes cluster fields into story rows (kept in separate hn_clusters).
    """
    max_gap = max(3 * poll_interval, 900)  # Allow at least 15 min gap for cold polls

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
            a_score = None
            a_comment = None
            a_norm = None
            rank_score = None
            is_capped = 0

            if history and ts_epoch > 0:
                target_epoch = ts_epoch - LOOKBACK_TARGET_SEC
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

                if dt_sec is not None and MIN_DT_SEC <= dt_sec <= max_gap:
                    dt_min = dt_sec / 60.0
                    dscore = max(0, score - score_prev)
                    dcomment = max(0, comments - comment_prev)

                    v_score = round(dscore / dt_min, 4)
                    v_comment = round(dcomment / dt_min, 4)

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

            # Precomputed display dimensions
            raw_title = str(snap.get("title") or "").strip()
            clean_title = fix_mojibake(raw_title)
            snap["title"] = clean_title

            url = str(snap.get("url") or "").strip()
            link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
            title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{clean_title}</a>'

            created_epoch = parse_timestamp_to_epoch(snap.get("created_at"))
            diff_sec = max(0, ts_epoch - created_epoch) if (ts_epoch > 0 and created_epoch > 0) else 0
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

            # Ensure cluster fields are NOT present in story records
            snap.pop("cluster_id", None)
            snap.pop("cluster_label", None)

            updated_records[orig_idx] = snap

            history.append({
                "epoch": ts_epoch,
                "score": score,
                "comments": comments,
                "dt_sec": dt_sec,
                "v_score": v_score,
                "v_comment": v_comment
            })

    return [r for r in updated_records if r is not None]


def generate_clusters_dataset(snapshots: list[dict], min_stories: int = 2, output_file: str = "hn_clusters.jsonl") -> list[dict]:
    """
    Generates a dedicated narrative clusters dataset:
    - Filters for the latest observation per post_id.
    - Groups stories by title similarity first (shared key phrases & semantic topics).
    - Strictly enforces min 2 stories per cluster (never singletons).
    - Writes records to hn_clusters.jsonl without polluting story rows.
    """
    # 1. Map latest stories
    latest_stories = {}
    for r in snapshots:
        if r.get("latest_flag") == 1:
            pid = str(r["post_id"])
            latest_stories[pid] = r

    if not latest_stories:
        # Fallback to latest snapshot per post_id
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

    # 1. Check thematic groups
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

    # 2. Dynamic title keyword overlap for remaining unassigned stories
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

    # Find global latest timestamp for cluster snapshot
    latest_ts = max((r.get("timestamp") for r in snapshots if r.get("timestamp")), default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

    # Build cluster records
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
        self.max_gap = max(3 * poll_interval, 900)
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

        # Precomputed display dimensions
        raw_title = str(snap.get("title") or "").strip()
        clean_title = fix_mojibake(raw_title)
        snap["title"] = clean_title

        url = str(snap.get("url") or "").strip()
        link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
        title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{clean_title}</a>'

        created_epoch = parse_timestamp_to_epoch(snap.get("created_at"))
        diff_sec = max(0, ts_epoch - created_epoch) if (ts_epoch > 0 and created_epoch > 0) else 0
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

        # Never write cluster columns into story snapshot rows
        snap.pop("cluster_id", None)
        snap.pop("cluster_label", None)

        hist.append({
            "epoch": ts_epoch,
            "score": score,
            "comments": comments,
            "dt_sec": dt_sec,
            "v_score": v_score,
            "v_comment": v_comment
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
