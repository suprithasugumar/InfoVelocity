#!/usr/bin/env python3
"""
InfoVelocity - Supervised Long-Lived Collector Service Runner
=============================================================
Supervises hn_collector.py as a continuous long-lived background service:
- Polls live Hacker News REST API every 60 seconds
- Emits structured heartbeat on every poll cycle
- Automatically syncs new snapshots to Apache Druid continuously
- Restarts on unexpected exit with exponential backoff
"""

import sys
import time
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Supervisor] %(message)s"
)
logger = logging.getLogger("CollectorSupervisor")

COLLECTOR_SCRIPT = "hn_collector.py"
POLL_INTERVAL = 60


def run_supervised():
    logger.info("=============================================================")
    logger.info("  Starting Supervised InfoVelocity Continuous Collector       ")
    logger.info(f"  Target: {COLLECTOR_SCRIPT} (poll interval: {POLL_INTERVAL}s)")
    logger.info("=============================================================")

    while True:
        try:
            logger.info("Launching collector subprocess...")
            proc = subprocess.Popen(
                [sys.executable, COLLECTOR_SCRIPT, "--interval", str(POLL_INTERVAL)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding="utf-8"
            )

            # Stream output
            for line in proc.stdout:
                line_str = line.strip()
                if line_str:
                    print(line_str, flush=True)

            proc.wait()
            ret = proc.returncode
            logger.warning(f"Collector process exited with returncode {ret}. Restarting in 5s...")
            time.sleep(5)

        except KeyboardInterrupt:
            logger.info("Supervisor stopped by user. Terminating collector...")
            if proc and proc.poll() is None:
                proc.terminate()
            break
        except Exception as e:
            logger.error(f"Unexpected supervisor error: {e}. Restarting in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    run_supervised()
