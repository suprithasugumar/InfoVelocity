#!/usr/bin/env python3
"""
InfoVelocity - Kinematics Processing & Narrative Clustering Pipeline
====================================================================
Computes instantaneous velocity (1st derivative) and acceleration (2nd derivative)
for each story across sequential time snapshots in Python before Druid ingestion.
Precomputes display columns (title_link, story_age, observation_time_ist, quadrant)
and groups stories posted in the last 6h into Narrative Clusters (cluster_id, cluster_label).

Key Kinematics Rules:
  - 5-Minute Lookback Window:
      Compares current snapshot against the snapshot closest to 5 minutes (300s) earlier
      (falling back to the immediately preceding snapshot if no ~5m snapshot exists).
  - dt_sec: Real elapsed seconds between current snapshot and chosen lookback snapshot.
  - Min-dt & Gap Control:
      If dt_sec < 20s or dt_sec > 3 * POLL_INTERVAL (900s): v_score, v_comment = None.
  - v_score = max(0, dscore) / (dt_sec / 60.0)  [points / min]
  - v_comment = max(0, dcomments) / (dt_sec / 60.0) [comments / min]
  - Acceleration Guard:
      Acceleration computed when dt_sec >= 20s and dscore >= 1.
  - Sane Cap:
      Flags any row capped by ACCELERATION_CAP_THRESHOLD (500.0 pts/min²) with is_capped = 1.
  - Precomputed Display Dimensions:
      * title_link: Clickable HTML link falling back to HN item URL
      * story_age: Elapsed time since publication (e.g. '45m', '1.5h', '2.0d')
      * observation_time_ist: Timestamp in IST (HH:mm:ss)
      * quadrant: 4-quadrant momentum classification
      * cluster_id & cluster_label: Domain and keyword semantic topic clustering
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
POLL_INTERVAL_DEFAULT = 300
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

TOPIC_PATTERNS = [
    (
        "cluster_ai",
        "AI & Large Language Models",
        {"ai", "llm", "gpt", "claude", "gemini", "openai", "anthropic", "neural", "transformer", "deepseek", "mistral", "agent", "agents", "model", "models", "prompt", "embedding", "embeddings", "llama"}
    ),
    (
        "cluster_security",
        "Cybersecurity & Vulnerabilities",
        {"security", "vulnerability", "vulnerabilities", "cve", "exploit", "exploits", "hacked", "breach", "ssh", "malware", "crypto", "cryptography", "ransomware", "backdoor", "0-day", "zero-day"}
    ),
    (
        "cluster_systems",
        "Operating Systems & Kernel",
        {"linux", "kernel", "bsd", "openbsd", "freebsd", "unix", "macos", "windows", "tmux", "terminal", "driver", "firmware", "hardware", "cpu", "arm", "risc-v", "gpu"}
    ),
    (
        "cluster_languages",
        "Programming Languages & Compilers",
        {"rust", "python", "golang", "c++", "typescript", "javascript", "compiler", "compilers", "type", "types", "interpreter", "llvm", "syntax", "wasm", "webassembly"}
    ),
    (
        "cluster_databases",
        "Databases & Infrastructure",
        {"database", "databases", "sqlite", "postgres", "postgresql", "mysql", "redis", "druid", "distributed", "storage", "sql", "cache", "kafka", "queue"}
    ),
    (
        "cluster_web",
        "Web Platforms & Browsers",
        {"browser", "browsers", "chrome", "firefox", "safari", "web", "html", "css", "frontend", "dom", "http", "api", "rest", "wasm"}
    ),
    (
        "cluster_business",
        "Startups & Tech Industry",
        {"startup", "startups", "business", "funding", "ipo", "revenue", "law", "court", "antitrust", "market", "economy", "layoffs", "investing"}
    ),
]


def extract_domain(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return ""
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""


def extract_keywords(title: str) -> set:
    """Extract normalized alphanumeric words from title, removing stopwords."""
    if not title:
        return set()
    words = re.findall(r'[a-zA-Z0-9\-\+\#]{3,}', title.lower())
    return {w for w in words if w not in STOPWORDS}


def assign_narrative_clusters(records: list[dict]) -> None:
    """
    Groups stories by semantic topic patterns, shared domains, or keyword similarity.
    Assigns 'cluster_id' and 'cluster_label' in-place to each record.
    """
    # 1. Map story details per post_id
    stories = {}
    for r in records:
        pid = str(r.get("post_id", ""))
        if not pid or pid in stories:
            continue
        url = str(r.get("url") or "")
        title = str(r.get("title") or "")
        domain = extract_domain(url)
        kws = extract_keywords(title)
        stories[pid] = {
            "title": title,
            "url": url,
            "domain": domain,
            "keywords": kws
        }

    # Count domain frequencies (excluding HN internal)
    domain_counts = Counter(
        s["domain"] for s in stories.values()
        if s["domain"] and "ycombinator.com" not in s["domain"]
    )

    story_clusters = {}
    for pid, s in stories.items():
        assigned = False
        kws = s["keywords"]
        title_lower = s["title"].lower()

        # Check curated high-value topic patterns first
        for cid, clabel, topic_words in TOPIC_PATTERNS:
            if any(w in kws or w in title_lower for w in topic_words):
                story_clusters[pid] = (cid, clabel)
                assigned = True
                break

        if assigned:
            continue

        # Check shared domain (count >= 2)
        domain = s["domain"]
        if domain and domain_counts[domain] >= 2:
            if "github.com" in domain:
                story_clusters[pid] = ("cluster_github", "GitHub & Open Source Tools")
            else:
                story_clusters[pid] = (f"cluster_domain_{domain.replace('.', '_')}", f"Domain: {domain}")
            assigned = True
            continue

        # Single domain or niche topic
        if domain:
            story_clusters[pid] = (f"cluster_{domain.replace('.', '_')}", f"Topic: {domain}")
        else:
            # First significant title keyword
            sig_words = [w for w in kws if len(w) >= 4]
            if sig_words:
                kw = sig_words[0].capitalize()
                story_clusters[pid] = (f"cluster_{sig_words[0]}", f"Topic: {kw} Discussions")
            else:
                story_clusters[pid] = ("cluster_general", "General Tech Discussions")

    # Assign cluster_id and cluster_label to all records
    for r in records:
        pid = str(r.get("post_id", ""))
        cid, clabel = story_clusters.get(pid, ("cluster_general", "General Tech Discussions"))
        r["cluster_id"] = cid
        r["cluster_label"] = clabel


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
    - 5-minute lookback kinematics (v_score, v_comment, a_score, a_comment, rank_score, a_norm)
    - Latest flag & eligibility
    - Precomputed display dimensions (title_link, story_age, observation_time_ist, quadrant)
    - Precomputed narrative clusters (cluster_id, cluster_label)
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
                    # 1. dt_sec >= MIN_ACCELERATION_DT_SEC
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

            # Precomputed display dimensions
            url = str(snap.get("url") or "").strip()
            title = str(snap.get("title") or "").strip()
            link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
            title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{title}</a>'

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
                    quadrant = "Fading / Dormant"
            elif v_score is not None and v_score > 0:
                quadrant = "Steady (Linear Growth)"
            else:
                quadrant = "Fading / Dormant"

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

    valid_records = [r for r in updated_records if r is not None]

    # Precompute narrative clusters across all records
    assign_narrative_clusters(valid_records)

    return valid_records


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

        # Precomputed display dimensions
        url = str(snap.get("url") or "").strip()
        title = str(snap.get("title") or "").strip()
        link_target = url if url else f"https://news.ycombinator.com/item?id={post_id}"
        title_link = f'<a href="{link_target}" target="_blank" rel="noopener noreferrer" style="color: #60a5fa; text-decoration: none; font-weight: 500;">{title}</a>'

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
                quadrant = "Fading / Dormant"
        elif v_score is not None and v_score > 0:
            quadrant = "Steady (Linear Growth)"
        else:
            quadrant = "Fading / Dormant"

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
