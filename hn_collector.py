#!/usr/bin/env python3
"""
InfoVelocity - Stage 1 & Snapshot Collector: Hacker News
=========================================================
Polls the Hacker News Firebase REST API:
1. Discovers stories from /v0/newstories, /v0/topstories, and /v0/beststories.
2. Tracks up to ~300 stories posted in the last 24h (type == 'story', not deleted, not dead, non-empty title).
3. Drops stories older than 24h from the active pool (keeping history in files).
4. Emits structured per-cycle logs: pool size, new stories added, stories dropped.
5. Emits structured heartbeats on every polling cycle for service liveness monitoring.
6. Automatically pre-computes kinematics and syncs snapshots & clusters to Apache Druid.
7. Isolated demo mode via --demo flag targeting hn_snapshots_sim (never mixing with real data).
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from dotenv import load_dotenv

from compute_kinematics import (
    StoryKinematicsTracker,
    compute_file_kinematics,
    fix_mojibake
)

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
INDEXER_URL = os.getenv("DRUID_OVERLORD_URL", "http://localhost:8081")
CONTAINER_NAME = "middlemanager"


def format_iso_timestamp(epoch: int | float | None) -> str:
    """Converts a unix timestamp to UTC ISO 8601 string."""
    if epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sync_to_druid(data_file: str, spec_file: str, datasource_name: str) -> bool:
    """Copies JSONL file to middlemanager container and triggers Druid batch ingestion."""
    if not os.path.exists(data_file):
        logger.warning(f"[Druid Sync] Data file {data_file} does not exist. Skipping sync.")
        return False

    target_path = f"/opt/shared/{os.path.basename(data_file)}"
    try:
        # Copy to container
        res = subprocess.run(
            ["docker", "cp", data_file, f"{CONTAINER_NAME}:{target_path}"],
            capture_output=True,
            text=True
        )
        if res.returncode != 0:
            logger.error(f"[Druid Sync] Failed to copy {data_file} to container: {res.stderr}")
            return False

        # Read ingestion spec
        if not os.path.exists(spec_file):
            logger.error(f"[Druid Sync] Spec file {spec_file} not found.")
            return False

        with open(spec_file, "r", encoding="utf-8") as f:
            spec = json.load(f)

        task_url = f"{INDEXER_URL}/druid/indexer/v1/task"
        resp = requests.post(task_url, json=spec, timeout=10)
        if resp.status_code == 200:
            task_id = resp.json().get("task")
            logger.info(f"[Druid Sync] Submitted ingestion task for '{datasource_name}' (Task ID: {task_id})")
            return True
        else:
            logger.warning(f"[Druid Sync] Task submission returned {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        logger.error(f"[Druid Sync] Exception during sync: {e}")
        return False


def is_valid_story(item_data: dict | None) -> bool:
    """Only keep items where type == 'story', not deleted, not dead, and title is non-empty."""
    if not item_data or not isinstance(item_data, dict):
        return False
    if item_data.get("deleted") or item_data.get("dead"):
        return False
    if item_data.get("type") != "story":
        return False
    title = item_data.get("title")
    if not title or not str(title).strip():
        return False
    return True


def fetch_candidate_story_ids(limit_per_endpoint: int = 150, timeout: int = 10) -> list[int]:
    """
    Fetches candidate story IDs from /v0/topstories, /v0/newstories, and /v0/beststories.
    Combines and deduplicates while preserving freshness and prominence.
    """
    endpoints = ["topstories.json", "newstories.json", "beststories.json"]
    ordered_ids = []
    seen = set()

    for ep in endpoints:
        url = f"{HN_BASE_URL}/{ep}"
        try:
            r = requests.get(url, timeout=timeout)
            if r.ok and isinstance(r.json(), list):
                data = r.json()
                for item_id in data[:limit_per_endpoint]:
                    if item_id not in seen:
                        seen.add(item_id)
                        ordered_ids.append(item_id)
        except Exception as e:
            logger.warning(f"Failed to fetch {ep}: {e}")

    return ordered_ids


def fetch_single_item(session: requests.Session, item_id: int | str, timeout: int = 10) -> tuple[str, dict | None]:
    """Fetches details for a single item using requests Session."""
    url = f"{HN_BASE_URL}/item/{item_id}.json"
    str_id = str(item_id)
    try:
        r = session.get(url, timeout=timeout)
        if r.ok and r.json():
            return str_id, r.json()
    except Exception:
        pass
    return str_id, None


def fetch_items_concurrent(item_ids: list[int | str], max_workers: int = 25, timeout: int = 10) -> dict[str, dict]:
    """Fetches item details concurrently in parallel threads."""
    results = {}
    if not item_ids:
        return results

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers, max_retries=2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_single_item, session, item_id, timeout) for item_id in item_ids]
            for fut in as_completed(futures):
                str_id, data = fut.result()
                if data:
                    results[str_id] = data
    finally:
        session.close()

    return results


def load_seen_ids_and_active_pool(
    events_file: str,
    snapshots_file: str,
    active_window_hours: int = 24,
    active_limit: int = 300
) -> tuple[set, OrderedDict]:
    """Reads existing catalog and snapshots to populate seen_ids and seed active_pool with stories < 24h old."""
    seen = set()
    active_pool = OrderedDict()
    now_epoch = datetime.now(timezone.utc).timestamp()
    cutoff_epoch = now_epoch - (active_window_hours * 3600)

    # 1. Load seen catalog IDs
    events_path = Path(events_file)
    if events_path.exists():
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        post_id = str(data.get("post_id", ""))
                        if post_id:
                            seen.add(post_id)
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
        except Exception as e:
            logger.warning(f"Could not read events file: {e}")

    # 2. Check snapshots for recently observed stories within 24h
    snaps_path = Path(snapshots_file)
    if snaps_path.exists():
        try:
            with open(snaps_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        post_id = str(data.get("post_id", ""))
                        if post_id:
                            seen.add(post_id)
                            ts_str = data.get("created_at")
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
        except Exception as e:
            logger.warning(f"Could not read snapshots file: {e}")

    # Prune to active_limit
    while len(active_pool) > active_limit:
        active_pool.popitem(last=False)

    logger.info(f"Loaded {len(seen)} existing post IDs for catalog deduplication.")
    logger.info(f"Seeded active pool with {len(active_pool)} recent stories within {active_window_hours}h window.")
    return seen, active_pool


def poll_cycle_live(
    events_file: str,
    snapshots_file: str,
    seen_ids: set,
    active_pool: OrderedDict,
    active_window_hours: int = 24,
    active_limit: int = 300,
    poll_interval: int = 60,
    auto_sync: bool = True,
    is_demo: bool = False
) -> tuple[int, int]:
    """
    Executes a single polling cycle:
    1. Prunes stories older than 24h from active pool.
    2. Fetches candidate stories from /v0/newstories, /v0/topstories, /v0/beststories.
    3. Fills active pool up to ~300 stories posted in the last 24h.
    4. Logs pool size, new stories added, stories dropped.
    5. Polls engagement metrics for all active stories and writes snapshots.
    6. Recomputes kinematics on snapshots and outputs hn_clusters.jsonl.
    7. Continuously syncs snapshots and clusters to Apache Druid.
    8. Emits heartbeat log.
    """
    now_dt = datetime.now(timezone.utc)
    current_snapshot_ts = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_epoch = now_dt.timestamp()
    cutoff_epoch = now_epoch - (active_window_hours * 3600)

    stories_dropped = 0
    new_stories_added = 0

    # --- Phase 1: Prune stories older than 24h from active tracking ---
    expired_ids = [pid for pid, created_epoch in active_pool.items() if created_epoch < cutoff_epoch]
    for pid in expired_ids:
        del active_pool[pid]
        stories_dropped += 1

    # --- Phase 2: Candidate Discovery from new, top, best ---
    needed = active_limit - len(active_pool)
    new_catalog_events = []

    # Fetch candidate IDs from newstories, topstories, and beststories
    candidate_ids = fetch_candidate_story_ids(limit_per_endpoint=150)
    untracked_candidates = [cid for cid in candidate_ids if str(cid) not in active_pool]

    if untracked_candidates:
        # Fetch batch of candidates to fill pool
        batch_to_fetch = untracked_candidates[:max(needed * 2, 60)]
        fetched_items = fetch_items_concurrent(batch_to_fetch, max_workers=25)

        for item_id in batch_to_fetch:
            if len(active_pool) >= active_limit:
                break
            str_id = str(item_id)
            item_data = fetched_items.get(str_id)
            if not is_valid_story(item_data):
                seen_ids.add(str_id)
                continue

            created_epoch = item_data.get("time") or now_epoch
            if created_epoch < cutoff_epoch:
                seen_ids.add(str_id)
                continue

            # Story is valid and < 24h old
            created_iso = format_iso_timestamp(created_epoch)
            clean_title = fix_mojibake(str(item_data.get("title") or "").strip())
            post_url = item_data.get("url") or f"https://news.ycombinator.com/item?id={str_id}"

            active_pool[str_id] = created_epoch

            if str_id not in seen_ids:
                seen_ids.add(str_id)
                new_stories_added += 1
                catalog_record = {
                    "post_id": str_id,
                    "timestamp": created_iso,
                    "created_at": created_iso,
                    "title": clean_title,
                    "body": str(item_data.get("text") or ""),
                    "author": str(item_data.get("by") or "[deleted]"),
                    "source": "hackernews",
                    "subreddit": "hackernews",
                    "url": str(post_url),
                    "score": int(item_data.get("score") or 0),
                    "num_comments": int(item_data.get("descendants") or 0)
                }
                new_catalog_events.append(catalog_record)

    # Persist newly cataloged stories
    if new_catalog_events and not is_demo:
        try:
            with open(events_file, "a", encoding="utf-8") as f:
                for post in new_catalog_events:
                    f.write(json.dumps(post, ensure_ascii=False) + "\n")
            logger.info(f"[Catalog] Saved {len(new_catalog_events)} new stories to {events_file}.")
        except IOError as e:
            logger.error(f"Failed to write to {events_file}: {e}")

    # Log pool size, new stories added, stories dropped
    logger.info(
        f"[Active Pool] Pool size: {len(active_pool)} | "
        f"New stories added: {new_stories_added} | "
        f"Stories dropped: {stories_dropped}"
    )

    # --- Phase 3: Engagement Snapshot Collection ---
    active_ids = list(active_pool.keys())
    active_item_data = fetch_items_concurrent(active_ids, max_workers=25)

    snapshots = []
    for str_id in active_ids:
        item_data = active_item_data.get(str_id)
        if not is_valid_story(item_data):
            if str_id in active_pool:
                del active_pool[str_id]
                stories_dropped += 1
            continue

        created_epoch = item_data.get("time") or active_pool.get(str_id, now_epoch)
        if created_epoch < cutoff_epoch:
            if str_id in active_pool:
                del active_pool[str_id]
                stories_dropped += 1
            continue

        created_iso = format_iso_timestamp(created_epoch)
        clean_title = fix_mojibake(str(item_data.get("title") or "").strip())
        post_url = item_data.get("url") or f"https://news.ycombinator.com/item?id={str_id}"

        snapshot_record = {
            "post_id": str_id,
            "timestamp": current_snapshot_ts,
            "created_at": created_iso,
            "title": clean_title,
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
            logger.info(f"[Snapshot] Recorded {len(snapshots)} engagement snapshots -> {snapshots_file} (ts={current_snapshot_ts}).")

            # Pre-compute kinematics across all snapshots and generate narrative clusters
            compute_file_kinematics(snapshots_file, poll_interval=poll_interval)

            # Auto-sync to Druid
            if auto_sync:
                ds_name = "hn_snapshots_sim" if is_demo else "hn_snapshots"
                spec_name = "druid_snapshots_sim_ingestion_spec.json" if is_demo else "druid_snapshots_ingestion_spec.json"
                sync_to_druid(snapshots_file, spec_name, ds_name)

                if not is_demo and os.path.exists("hn_clusters.jsonl"):
                    sync_to_druid("hn_clusters.jsonl", "druid_clusters_ingestion_spec.json", "hn_clusters")

        except IOError as e:
            logger.error(f"Failed to write to {snapshots_file}: {e}")

    # Emit Heartbeat log
    logger.info(f"[HEARTBEAT] Collector active | Polled at {current_snapshot_ts} | Pool: {len(active_pool)} stories | Status: HEALTHY | Mode: {'DEMO/SIM' if is_demo else 'LIVE'}")

    return new_stories_added, len(snapshots)


def main():
    parser = argparse.ArgumentParser(
        description="InfoVelocity - Hacker News Continuous Collector Service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_FILE", "hn_events.jsonl"),
        help="Path to immutable catalog JSONL file."
    )
    parser.add_argument(
        "--snapshots-output",
        default=None,
        help="Path to engagement snapshots JSONL file (default: hn_snapshots.jsonl or hn_snapshots_sim.jsonl if demo)."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
        help="Interval in seconds between polling cycles."
    )
    parser.add_argument(
        "--active-limit",
        type=int,
        default=int(os.getenv("ACTIVE_STORY_LIMIT", "300")),
        help="Maximum active stories to track for engagement snapshots (up to ~300)."
    )
    parser.add_argument(
        "--active-window-hours",
        type=int,
        default=int(os.getenv("ACTIVE_WINDOW_HOURS", "24")),
        help="Active story window in hours before a story stops being snapshot."
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Number of polling cycles to run (0 = run continuously)."
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Disable automatic continuous sync to Druid."
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run in isolated demo/simulated mode. Writes to hn_snapshots_sim and never mixes with real data."
    )
    args = parser.parse_args()

    if args.demo:
        snapshots_file = args.snapshots_output or "hn_snapshots_sim.jsonl"
        mode_label = "DEMO / SIMULATED (Target: hn_snapshots_sim)"
    else:
        snapshots_file = args.snapshots_output or os.getenv("SNAPSHOT_FILE", "hn_snapshots.jsonl")
        mode_label = "LIVE Hacker News API (Target: hn_snapshots)"

    logger.info("=================================================================")
    logger.info("  InfoVelocity - Hacker News Continuous Collector Service         ")
    logger.info("=================================================================")
    logger.info(f"API Endpoint         : {HN_BASE_URL}")
    logger.info(f"Catalog File         : {args.output}")
    logger.info(f"Snapshots File       : {snapshots_file}")
    logger.info(f"Polling Interval     : {args.interval} seconds")
    logger.info(f"Active Story Limit   : {args.active_limit}")
    logger.info(f"Active Window        : {args.active_window_hours} hours")
    logger.info(f"Mode                 : {mode_label}")
    logger.info(f"Auto Druid Sync      : {'Disabled' if args.no_sync else 'Continuous'}")
    logger.info("=================================================================")

    seen_ids, active_pool = load_seen_ids_and_active_pool(
        args.output,
        snapshots_file,
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

            new_ev, new_snap = poll_cycle_live(
                args.output,
                snapshots_file,
                seen_ids,
                active_pool,
                active_window_hours=args.active_window_hours,
                active_limit=args.active_limit,
                poll_interval=args.interval,
                auto_sync=not args.no_sync,
                is_demo=args.demo
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
