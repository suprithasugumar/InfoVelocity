#!/usr/bin/env python3
import json
from collections import defaultdict
from datetime import datetime

with open("hn_snapshots.jsonl", "r", encoding="utf-8") as f:
    snapshots = [json.loads(line) for line in f if line.strip()]

# 1. Max a_score across all snapshots
valid_a_snaps = [s for s in snapshots if s.get("a_score") is not None]
max_a_snap = max(valid_a_snaps, key=lambda s: s["a_score"])
print("=== 1. Max a_score Information ===")
print("Max a_score :", max_a_snap["a_score"])
print("Post ID     :", max_a_snap["post_id"])
print("Title       :", max_a_snap["title"])
print("dt_sec      :", max_a_snap["dt_sec"])
print("v_score     :", max_a_snap["v_score"])
print("is_capped   :", max_a_snap.get("is_capped", 0))
print()

# 2. Breakout dataset row count (in latest eligible 6h)
times = [datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")).timestamp() for s in snapshots]
max_t = max(times)
cutoff_6h = max_t - 6 * 3600

latest_eligible_last_6h = [
    s for s in snapshots
    if s.get("latest_flag") == 1 
    and s.get("eligible") == 1
    and datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")).timestamp() >= cutoff_6h
]
breakouts_6h = [s for s in latest_eligible_last_6h if (s.get("a_score") or 0) > 0]

print("=== 2. Breakout Dataset Row Count ===")
print("Latest eligible stories in last 6h:", len(latest_eligible_last_6h))
print("Breakout dataset row count in last 6h (a_score > 0):", len(breakouts_6h))
print()

# 3. Leaderboard velocity column confirmation for stories with 2+ snapshots
by_post = defaultdict(list)
for s in snapshots:
    by_post[s["post_id"]].append(s)

multi_snap_stories = {pid: snaps for pid, snaps in by_post.items() if len(snaps) >= 2}
leaderboard_candidates = [
    s for s in latest_eligible_last_6h
    if len(by_post[s["post_id"]]) >= 2
]

na_velocity_count = sum(1 for s in leaderboard_candidates if s.get("v_score") is None)
print("=== 3. Leaderboard Velocity Confirmation ===")
print("Total stories tracked:", len(by_post))
print("Stories with 2+ snapshots:", len(multi_snap_stories))
print("Stories on leaderboard with 2+ snapshots:", len(leaderboard_candidates))
print("Stories on leaderboard with 2+ snapshots having N/A velocity:", na_velocity_count)
print()

# 4. Direct Druid SQL endpoint verification
import requests
print("=== 4. Direct Druid SQL OLAP Queries ===")
q_max = 'SELECT MAX(a_score) as max_a FROM "hn_snapshots"'
r_max = requests.post("http://localhost:8888/druid/v2/sql", json={"query": q_max}).json()
print("Druid MAX(a_score) aggregation:", r_max)

q_breakout = 'SELECT COUNT(*) as breakout_count FROM "hn_snapshots" WHERE "__time" >= (SELECT MAX("__time") - INTERVAL \'6\' HOUR FROM "hn_snapshots") AND latest_flag = 1 AND eligible = 1 AND a_score > 0'
r_breakout = requests.post("http://localhost:8888/druid/v2/sql", json={"query": q_breakout}).json()
print("Druid Breakout row count (last 6h):", r_breakout)

q_null_v = 'SELECT COUNT(*) as null_v_count FROM "hn_snapshots" WHERE "__time" >= (SELECT MAX("__time") - INTERVAL \'6\' HOUR FROM "hn_snapshots") AND latest_flag = 1 AND eligible = 1 AND dt_sec IS NOT NULL AND v_score IS NULL'
r_null_v = requests.post("http://localhost:8888/druid/v2/sql", json={"query": q_null_v}).json()
print("Druid Null v_score count (multi-snapshot eligible):", r_null_v)

