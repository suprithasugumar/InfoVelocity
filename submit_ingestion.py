#!/usr/bin/env python3
"""
InfoVelocity - Stage 2: Druid Batch Ingestion Submitter
=======================================================
Copies JSONL data into Druid's shared Docker volume and submits
the native batch ingestion task to the Druid router / indexer API.

Usage:
  python submit_ingestion.py                     # Ingests hn_events.jsonl -> hn_events
  python submit_ingestion.py --snapshots         # Ingests hn_snapshots.jsonl -> hn_snapshots
  python submit_ingestion.py --data <file> --spec <spec>
"""

import os
import sys
import time
import argparse
import subprocess
import requests
import json

INDEXER_URL = os.getenv("DRUID_OVERLORD_URL", "http://localhost:8081")
ROUTER_URL = os.getenv("DRUID_ROUTER_URL", "http://localhost:8888")
CONTAINER_NAME = "middlemanager"


def copy_data_to_container(data_file: str, container_target_path: str):
    """Copies data file into the Druid container's shared volume."""
    if not os.path.exists(data_file):
        print(f"[ERROR] Data file {data_file} not found. Run hn_collector.py first!")
        sys.exit(1)

    print(f"[*] Copying {data_file} to container {CONTAINER_NAME}:{container_target_path}...")
    res = subprocess.run(
        ["docker", "cp", data_file, f"{CONTAINER_NAME}:{container_target_path}"],
        capture_output=True,
        text=True
    )
    if res.returncode != 0:
        print(f"[ERROR] Failed to copy file to container: {res.stderr}")
        sys.exit(1)
    print(f"[SUCCESS] {data_file} synced to Druid container shared volume.")


def submit_task(spec_file: str):
    """Submits ingestion spec to Druid and polls for task completion."""
    if not os.path.exists(spec_file):
        print(f"[ERROR] Spec file {spec_file} not found.")
        sys.exit(1)

    with open(spec_file, "r", encoding="utf-8") as f:
        spec = json.load(f)

    datasource = spec.get("spec", {}).get("dataSchema", {}).get("dataSource", "hn_events")
    task_url = f"{INDEXER_URL}/druid/indexer/v1/task"
    print(f"[*] Submitting ingestion task for datasource '{datasource}' to {task_url}...")

    resp = requests.post(task_url, json=spec)
    if resp.status_code != 200:
        print(f"[ERROR] Ingestion submission failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    task_id = resp.json().get("task")
    print(f"[SUCCESS] Ingestion task submitted! Task ID: {task_id}")

    # Poll status
    status_url = f"{task_url}/{task_id}/status"
    print(f"[*] Polling task status...")
    while True:
        s_resp = requests.get(status_url)
        if s_resp.status_code == 200:
            status_obj = s_resp.json().get("status", {})
            state = status_obj.get("status")
            print(f"    Current state: {state} (duration: {status_obj.get('duration', 0)}ms)")
            if state == "SUCCESS":
                print(f"[SUCCESS] Druid ingestion completed successfully!")
                break
            elif state in ("FAILED", "CANCELED"):
                print(f"[FAILED] Druid task finished with state {state}: {status_obj.get('errorMsg')}")
                sys.exit(1)
        time.sleep(2)

    # Verify datasource exists
    ds_url = f"{ROUTER_URL}/druid/v2/datasources"
    ds_resp = requests.get(ds_url)
    if ds_resp.status_code == 200:
        datasources = ds_resp.json()
        print(f"[VERIFIED] Active datasources: {datasources}")

    # Verify count via SQL
    sql_url = f"{ROUTER_URL}/druid/v2/sql"
    sql_resp = requests.post(sql_url, json={"query": f'SELECT COUNT(*) AS total_records FROM "{datasource}"'})
    if sql_resp.status_code == 200:
        count = sql_resp.json()[0].get("total_records")
        print(f"[VERIFIED] Datasource '{datasource}' now contains {count} records queryable via Druid SQL.")
    else:
        print(f"[WARNING] SQL verification query returned {sql_resp.status_code}: {sql_resp.text}")


def main():
    parser = argparse.ArgumentParser(description="Submit batch ingestion specs to Apache Druid.")
    parser.add_argument("--snapshots", action="store_true", help="Ingest hn_snapshots.jsonl using druid_snapshots_ingestion_spec.json")
    parser.add_argument("--clusters", action="store_true", help="Ingest hn_clusters.jsonl using druid_clusters_ingestion_spec.json")
    parser.add_argument("--data", default=None, help="Custom data file path")
    parser.add_argument("--spec", default=None, help="Custom spec file path")
    args = parser.parse_args()

    if args.snapshots:
        data_file = "hn_snapshots.jsonl"
        spec_file = "druid_snapshots_ingestion_spec.json"
        target_path = "/opt/shared/hn_snapshots.jsonl"
    elif args.clusters:
        data_file = "hn_clusters.jsonl"
        spec_file = "druid_clusters_ingestion_spec.json"
        target_path = "/opt/shared/hn_clusters.jsonl"
    elif args.data and args.spec:
        data_file = args.data
        spec_file = args.spec
        target_path = f"/opt/shared/{os.path.basename(data_file)}"
    else:
        data_file = "hn_events.jsonl"
        spec_file = "druid_ingestion_spec.json"
        target_path = "/opt/shared/hn_events.jsonl"

    if args.snapshots or (data_file and "snapshots" in data_file):
        try:
            from compute_kinematics import compute_file_kinematics
            print(f"[*] Pre-computing kinematics for {data_file}...")
            compute_file_kinematics(data_file)
        except Exception as e:
            print(f"[WARN] Failed to pre-compute kinematics: {e}")

    copy_data_to_container(data_file, target_path)
    submit_task(spec_file)

    print("\n" + "=" * 80)
    print("  INFOVELOCITY WEB DASHBOARD:")
    print("  URL: http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
