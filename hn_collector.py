#!/usr/bin/env python3
"""
InfoVelocity - Stage 1 & Snapshot Collector: Hacker News
=========================================================
Polls the Hacker News Firebase REST API:
1. Discovers newly submitted stories and appends them to hn_events.jsonl (immutable catalog).
2. Maintains an active pool of recent stories within a sliding time window.
3. Periodically re-fetches engagement metrics for all active stories and writes
   timestamped observations to hn_snapshots.jsonl for velocity & acceleration tracking.

Key Features:
- Dual-target output:
  * hn_events.jsonl: Immutable first-seen story catalog (deduplicated by post_id)
  * hn_snapshots.jsonl: Time-series engagement snapshots (multiple observations per post_id)
- Observation timestamp ('timestamp') vs. story publication timestamp ('created_at')
- Configurable active window hours and story limit
- Offline demo mode with simulated engagement growth across polling cycles
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup structured logging
log_file = os.getenv("LOG_FILE", "collector.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8")
    ]
)
logger = logging.getLogger("InfoVelocityHNCollector")

HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"


def load_seen_ids_and_active_pool(events_file: str, active_window_hours: int = 12, active_limit: int = 100) -> tuple[set, OrderedDict]:
    """
    Reads existing events file to:
    1. Load all historical post_ids into seen_ids to prevent duplicate catalog entries.
    2. Seed the active story pool with recent stories within the active window.
    """
    seen = set()
    active_pool = OrderedDict()
    events_path = Path(events_file)
    if not events_path.exists():
        return seen, active_pool

    now_epoch = datetime.now(timezone.utc).timestamp()
    cutoff_epoch = now_epoch - (active_window_hours * 3600)

    try:
        with open(events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    post_id = str(data.get("post_id", ""))
                    if not post_id:
                        continue
                    seen.add(post_id)

                    # Extract timestamp or created_at for active pool qualification
                    ts_str = data.get("created_at") or data.get("timestamp")
                    if ts_str:
                        try:
                            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            story_epoch = dt.timestamp()
                            if story_epoch >= cutoff_epoch:
                                active_pool[post_id] = story_epoch
                        except ValueError:
                            pass
                except json.JSONDecodeError:
                    continue

        # Enforce max capacity on seeded active pool (keep newest)
        if len(active_pool) > active_limit:
            excess = len(active_pool) - active_limit
            for _ in range(excess):
                active_pool.popitem(last=False)

        logger.info(f"Loaded {len(seen)} existing post IDs from {events_file} for catalog deduplication.")
        logger.info(f"Seeded active pool with {len(active_pool)} recent stories within {active_window_hours}h window.")
    except Exception as e:
        logger.warning(f"Could not read existing events file: {e}")

    return seen, active_pool


def fetch_new_story_ids(limit: int = 25, timeout: int = 10) -> list:
    """Fetches latest story IDs from Hacker News /v0/newstories.json endpoint."""
    url = f"{HN_BASE_URL}/newstories.json"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        story_ids = response.json()
        if isinstance(story_ids, list):
            return story_ids[:limit]
        return []
    except Exception as e:
        logger.error(f"Failed to fetch new stories from Hacker News API: {e}")
        return []


def fetch_item_details(item_id: str | int, timeout: int = 10) -> dict | None:
    """Fetches details for a specific item from Hacker News /v0/item/{id}.json."""
    url = f"{HN_BASE_URL}/item/{item_id}.json"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch item {item_id} details: {e}")
        return None


def format_iso_timestamp(epoch: int | float | None) -> str:
    """Converts a unix timestamp to UTC ISO 8601 string."""
    if epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def prune_active_pool(active_pool: OrderedDict, active_window_hours: int = 12, active_limit: int = 100) -> int:
    """Removes expired stories and enforces maximum capacity on the active pool."""
    now_epoch = datetime.now(timezone.utc).timestamp()
    cutoff_epoch = now_epoch - (active_window_hours * 3600)

    # Remove stories older than active window
    expired_ids = [pid for pid, created_epoch in active_pool.items() if created_epoch < cutoff_epoch]
    for pid in expired_ids:
        del active_pool[pid]

    # Enforce limit (evict oldest stories)
    while len(active_pool) > active_limit:
        active_pool.popitem(last=False)

    return len(expired_ids)


def poll_cycle_live(
    events_file: str,
    snapshots_file: str,
    seen_ids: set,
    active_pool: OrderedDict,
    posts_per_poll: int = 25,
    active_window_hours: int = 12,
    active_limit: int = 100
) -> tuple[int, int]:
    """
    Executes a dual-phase polling cycle:
    1. Discovery: Find new stories, append to hn_events.jsonl, and register in active_pool.
    2. Snapshot: Re-poll all stories in active_pool and append current metrics to hn_snapshots.jsonl.
    """
    now_dt = datetime.now(timezone.utc)
    current_snapshot_ts = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_epoch = now_dt.timestamp()

    # --- Phase 1: Discovery ---
    logger.info(f"[Discovery] Fetching newest story IDs (limit={posts_per_poll})...")
    candidate_ids = fetch_new_story_ids(limit=posts_per_poll)
    new_catalog_events = []

    for item_id in candidate_ids:
        str_id = str(item_id)
        if str_id in seen_ids:
            continue

        item_data = fetch_item_details(item_id)
        if not item_data or item_data.get("deleted") or not item_data.get("id"):
            seen_ids.add(str_id)
            continue

        created_epoch = item_data.get("time") or now_epoch
        created_iso = format_iso_timestamp(created_epoch)
        post_url = item_data.get("url") or f"https://news.ycombinator.com/item?id={str_id}"

        # Catalog event record (immutable first-seen)
        catalog_record = {
            "post_id": str_id,
            "timestamp": created_iso,
            "created_at": created_iso,
            "title": str(item_data.get("title") or ""),
            "body": str(item_data.get("text") or ""),
            "author": str(item_data.get("by") or "[deleted]"),
            "source": "hackernews",
            "subreddit": "hackernews",
            "url": str(post_url),
            "score": int(item_data.get("score") or 0),
            "num_comments": int(item_data.get("descendants") or 0)
        }
        new_catalog_events.append(catalog_record)
        seen_ids.add(str_id)
        active_pool[str_id] = created_epoch

    if new_catalog_events:
        try:
            with open(events_file, "a", encoding="utf-8") as f:
                for post in new_catalog_events:
                    f.write(json.dumps(post, ensure_ascii=False) + "\n")
            logger.info(f"[Catalog] Saved {len(new_catalog_events)} new stories to {events_file}.")
        except IOError as e:
            logger.error(f"Failed to write to {events_file}: {e}")
    else:
        logger.info("[Catalog] No new stories discovered in this cycle.")

    # --- Prune Active Pool ---
    pruned_count = prune_active_pool(active_pool, active_window_hours, active_limit)
    if pruned_count > 0:
        logger.info(f"[Active Pool] Pruned {pruned_count} expired stories from active tracking.")

    logger.info(f"[Snapshot] Polling engagement for {len(active_pool)} active stories...")

    # --- Phase 2: Snapshots ---
    snapshots = []
    for str_id in list(active_pool.keys()):
        item_data = fetch_item_details(str_id)
        if not item_data or item_data.get("deleted"):
            continue

        created_epoch = item_data.get("time") or active_pool.get(str_id, now_epoch)
        created_iso = format_iso_timestamp(created_epoch)
        post_url = item_data.get("url") or f"https://news.ycombinator.com/item?id={str_id}"

        snapshot_record = {
            "post_id": str_id,
            "timestamp": current_snapshot_ts,  # Current observation timestamp
            "created_at": created_iso,         # Original publication timestamp
            "title": str(item_data.get("title") or ""),
            "author": str(item_data.get("by") or "[deleted]"),
            "source": "hackernews",
            "subreddit": "hackernews",
            "url": str(post_url),
            "score": int(item_data.get("score") or 0),
            "num_comments": int(item_data.get("descendants") or 0)
        }
        snapshots.append(snapshot_record)

    if snapshots:
        try:
            with open(snapshots_file, "a", encoding="utf-8") as f:
                for snap in snapshots:
                    f.write(json.dumps(snap, ensure_ascii=False) + "\n")
            logger.info(f"[Snapshot] Saved {len(snapshots)} engagement snapshots to {snapshots_file} (ts={current_snapshot_ts}).")
        except IOError as e:
            logger.error(f"Failed to write to {snapshots_file}: {e}")

    return len(new_catalog_events), len(snapshots)


# In-memory mock store for demo simulation across cycles
_DEMO_STORY_STORE = {}


def poll_cycle_demo(
    events_file: str,
    snapshots_file: str,
    seen_ids: set,
    active_pool: OrderedDict,
    active_window_hours: int = 12,
    active_limit: int = 100
) -> tuple[int, int]:
    """Generates simulated stories and multi-timestamp snapshots with growing scores/comments."""
    import random

    now_dt = datetime.now(timezone.utc)
    current_snapshot_ts = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_epoch = now_dt.timestamp()

    sample_templates = [
        ("Show HN: Fast local vector search in pure C", "https://github.com/example/fast-vector-c", "c_optimizer"),
        ("Ask HN: What are your favorite lightweight monitoring stacks?", None, "devops_builder"),
        ("PostgreSQL 18 query planner optimizations and index skipping", "https://example-blog.org/postgres-18-planner", "db_enthusiast"),
        ("Why we migrated our data pipeline from batch to streaming Druid", "https://tech-insights.io/streaming-druid-pipeline", "data_lead"),
        ("A deep dive into modern browser layout engines", "https://web-internals.dev/layout-engine-deep-dive", "frontend_nerd")
    ]

    # Discovery: create 2-3 new stories per cycle in demo
    new_catalog_events = []
    num_new = 3
    for _ in range(num_new):
        item_id = str(random.randint(49780000, 49789999))
        if item_id in seen_ids:
            continue

        title, url, author = random.choice(sample_templates)
        initial_score = random.randint(1, 5)
        initial_comments = random.randint(0, 2)
        created_iso = current_snapshot_ts

        _DEMO_STORY_STORE[item_id] = {
            "title": title,
            "url": url if url else f"https://news.ycombinator.com/item?id={item_id}",
            "author": author,
            "created_iso": created_iso,
            "created_epoch": now_epoch,
            "score": initial_score,
            "num_comments": initial_comments
        }

        catalog_record = {
            "post_id": item_id,
            "timestamp": created_iso,
            "created_at": created_iso,
            "title": title,
            "body": "Demo story body text.",
            "author": author,
            "source": "hackernews",
            "subreddit": "hackernews",
            "url": _DEMO_STORY_STORE[item_id]["url"],
            "score": initial_score,
            "num_comments": initial_comments
        }
        new_catalog_events.append(catalog_record)
        seen_ids.add(item_id)
        active_pool[item_id] = now_epoch

    if new_catalog_events:
        with open(events_file, "a", encoding="utf-8") as f:
            for ev in new_catalog_events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    # Prune active pool
    prune_active_pool(active_pool, active_window_hours, active_limit)

    # Snapshot generation: increment score and comments for all active stories
    snapshots = []
    for str_id in list(active_pool.keys()):
        story = _DEMO_STORY_STORE.get(str_id)
        if not story:
            continue

        # Simulate engagement growth over time
        story["score"] += random.randint(2, 15)
        story["num_comments"] += random.randint(0, 4)

        snapshot_record = {
            "post_id": str_id,
            "timestamp": current_snapshot_ts,  # Current snapshot timestamp
            "created_at": story["created_iso"],
            "title": story["title"],
            "author": story["author"],
            "source": "hackernews",
            "subreddit": "hackernews",
            "url": story["url"],
            "score": story["score"],
            "num_comments": story["num_comments"]
        }
        snapshots.append(snapshot_record)

    with open(snapshots_file, "a", encoding="utf-8") as f:
        for snap in snapshots:
            f.write(json.dumps(snap, ensure_ascii=False) + "\n")

    logger.info(f"[DEMO MODE] Appended {len(new_catalog_events)} new stories to {events_file} and {len(snapshots)} snapshots to {snapshots_file}.")
    return len(new_catalog_events), len(snapshots)


def main():
    parser = argparse.ArgumentParser(
        description="InfoVelocity - Stage 1 & Snapshot Collector: Hacker News",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_FILE", "hn_events.jsonl"),
        help="Path to immutable catalog JSONL file."
    )
    parser.add_argument(
        "--snapshots-output",
        default=os.getenv("SNAPSHOT_FILE", "hn_snapshots.jsonl"),
        help="Path to engagement snapshots JSONL file."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("POLL_INTERVAL_SECONDS", "300")),
        help="Interval in seconds between polling cycles."
    )
    parser.add_argument(
        "--limit",
        "--posts-per-poll",
        dest="limit",
        type=int,
        default=int(os.getenv("POSTS_PER_POLL", "25")),
        help="Maximum newest stories to discover per cycle."
    )
    parser.add_argument(
        "--active-limit",
        type=int,
        default=int(os.getenv("ACTIVE_STORY_LIMIT", "100")),
        help="Maximum active stories to track for engagement snapshots."
    )
    parser.add_argument(
        "--active-window-hours",
        type=int,
        default=int(os.getenv("ACTIVE_WINDOW_HOURS", "12")),
        help="Active story window in hours before a story stops being snapshot."
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Number of polling cycles to run (0 = run continuously)."
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in demo mode generating sample stories and multi-snapshot engagement curves."
    )

    args = parser.parse_args()

    logger.info("=================================================================")
    logger.info("  InfoVelocity - Hacker News Snapshot & Event Collector          ")
    logger.info("=================================================================")
    logger.info(f"API Endpoint         : {HN_BASE_URL}")
    logger.info(f"Events Catalog File  : {args.output}")
    logger.info(f"Snapshots File       : {args.snapshots_output}")
    logger.info(f"Polling Interval     : {args.interval} seconds")
    logger.info(f"Posts Per Poll       : {args.limit}")
    logger.info(f"Active Story Limit   : {args.active_limit}")
    logger.info(f"Active Window Hours  : {args.active_window_hours} hours")
    logger.info(f"Cycles Target        : {'Continuous' if args.cycles == 0 else args.cycles}")
    logger.info(f"Mode                 : {'Demo (Simulated)' if args.demo else 'Live Hacker News API'}")

    # Seed seen IDs and active story pool from existing events catalog
    seen_ids, active_pool = load_seen_ids_and_active_pool(
        args.output,
        active_window_hours=args.active_window_hours,
        active_limit=args.active_limit
    )

    cycle_count = 0
    total_new_events = 0
    total_snapshots = 0

    try:
        while True:
            cycle_count += 1
            logger.info(f"--- Starting Polling Cycle {cycle_count} ---")

            if args.demo:
                new_ev, new_snap = poll_cycle_demo(
                    args.output,
                    args.snapshots_output,
                    seen_ids,
                    active_pool,
                    active_window_hours=args.active_window_hours,
                    active_limit=args.active_limit
                )
            else:
                new_ev, new_snap = poll_cycle_live(
                    args.output,
                    args.snapshots_output,
                    seen_ids,
                    active_pool,
                    posts_per_poll=args.limit,
                    active_window_hours=args.active_window_hours,
                    active_limit=args.active_limit
                )

            total_new_events += new_ev
            total_snapshots += new_snap

            logger.info(
                f"Cycle {cycle_count} finished: +{new_ev} new stories cataloged, "
                f"+{new_snap} snapshots taken (Active pool: {len(active_pool)})"
            )

            if args.cycles > 0 and cycle_count >= args.cycles:
                logger.info(f"Completed requested {args.cycles} cycles. Exiting.")
                break

            logger.info(f"Sleeping for {args.interval} seconds until next cycle (Press Ctrl+C to stop)...")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("\nCollector stopped by user (Ctrl+C).")
        logger.info(f"Total new stories cataloged: {total_new_events}")
        logger.info(f"Total snapshots recorded   : {total_snapshots}")
        sys.exit(0)


if __name__ == "__main__":
    main()
