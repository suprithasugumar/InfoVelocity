# InfoVelocity: Real-Time Narrative Velocity & Acceleration Tracker

**InfoVelocity** is a high-performance analytical pipeline and business intelligence system designed to monitor, measure, and visualize the real-time propagation rate ("velocity") and rate of surge ("acceleration") of online discussions, technical narratives, and breaking topics.

Using the **Hacker News Firebase REST API**, **Apache Druid** (time-series OLAP engine), and **Apache Superset** (data visualization), InfoVelocity shifts social data analysis from static engagement counts (total upvotes/comments) to **instantaneous first and second derivative momentum metrics**.

---

> [!IMPORTANT]
> ### Epistemic Scope & Veracity Disclaimer
> **InfoVelocity is a measurement engine for information diffusion dynamics and engagement momentum.** 
> The system tracks *how fast* discussions spread and *how rapidly* attention accelerates across social networks. **It does not evaluate, classify, or determine whether a story, headline, or claim is factually true or false.** High velocity and positive acceleration indicate viral interest and rapid discussion growth, not factual validity.

---

## Architecture Overview

```
                      +-----------------------------------+
                      |   Hacker News Firebase REST API   |
                      |   (/v0/newstories, /v0/item/{id}) |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------------------------+
                      |          hn_collector.py          |
                      | - Active Pool (Sliding Window)    |
                      | - Post Deduplication & Snapshotting|
                      +--------+-----------------+--------+
                               |                 |
                               v                 v
            +----------------------+   +-----------------------+
            |   hn_events.jsonl    |   |  hn_snapshots.jsonl   |
            | (Immutable Catalog)  |   | (Time-Series Metrics) |
            +----------+-----------+   +-----------+-----------+
                       |                           |
                       +-------------+-------------+
                                     |
                                     v
                      +-----------------------------------+
                      |      Apache Druid OLAP Engine     |
                      | - Native Parallel Batch Ingestion |
                      | - Segmented Columnar Time-Series  |
                      | - Datasource: "hn_snapshots"      |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------------------------+
                      |    Velocity & Acceleration SQL    |
                      | - 1st Derivative: Engagement / Δt |
                      | - 2nd Derivative: ΔVelocity / Δt  |
                      | - Window LAG() & TIMESTAMPDIFF()  |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------------------------+
                      |    Apache Superset BI Platform    |
                      | - Physical & Virtual SQL Datasets |
                      | - Interactive Real-Time Dashboard |
                      +-----------------+-----------------+
                                        |
                                        v
                      +-----------------------------------+
                      |       InfoVelocity Dashboard      |
                      | 1. Macro Velocity Trend (5-min)   |
                      | 2. Fastest Growing Stories (v)    |
                      | 3. Breakout Acceleration Monitor  |
                      | 4. Story Engagement Progression   |
                      +-----------------------------------+
```

---

## Problem Statement

Standard social listening tools and dashboards present aggregate metrics: cumulative upvotes, total comments, or total mentions. These metrics suffer from critical shortcomings when monitoring rapid narrative shifts:

1. **Lagging Indicators**: High cumulative scores only emerge *after* a narrative has already peaked and saturated the network.
2. **Inability to Detect Early Surges**: A post rising from 5 to 50 comments in 3 minutes represents an explosive 15 comments/min velocity, yet it remains buried beneath older stories with hundreds of static comments.
3. **No Acceleration Sensing**: A narrative might have high velocity that is decelerating (losing steam) or low velocity that is aggressively accelerating (a breakout event).

**InfoVelocity solves this by treating engagement as a physical kinematics problem**, sampling time-series observations to calculate **Instantaneous Velocity ($v$)** and **True Acceleration ($a$)** in real time.

---

## Architectural Pivot: Reddit to Hacker News

InfoVelocity was originally prototyped with a Reddit ingestion poller (`reddit_poller.py`). In late 2025/2026, Reddit revised its developer terms under the **"Responsible Builder Policy"**, removing self-service API token registration and creating access blockers for automated pipelines.

### Why the Core Architecture Remained Intact:
1. **Generic Engagement Data Model**: InfoVelocity was designed around universal social graph primitives:
   $$\text{Observation} = (\text{post\_id}, \text{timestamp}, \text{created\_at}, \text{score}, \text{num\_comments}, \text{title}, \text{author}, \text{url})$$
2. **Schema Uniformity**: The Hacker News Firebase REST API provides exact 1-to-1 counterparts:
   - `id` $\to$ `post_id`
   - `score` $\to$ `score` (upvotes)
   - `descendants` $\to$ `num_comments`
   - `time` $\to$ `created_at` (story submission time)
   - Client observation timestamp $\to$ `__time` / `timestamp`
3. **Downstream Isolation**: Apache Druid ingestion specs, SQL derivative formulas, and Apache Superset dashboards operate strictly on the abstract metrics schema. The pivot was accomplished purely by swapping the collector adapter without altering the storage or analytical layers.
4. **Backward Compatibility**: A `subreddit` dimension aliased to `"hackernews"` was preserved in collector outputs and ingestion specs to ensure seamless schema continuity.

*(Note: Legacy Reddit prototype scripts and data have been safely archived in the `legacy/` directory).*

---

## Mathematical Model

InfoVelocity models user interaction along discrete snapshot observations $(t_0, t_1, \dots, t_n)$ for each tracked story.

### 1. Instantaneous Velocity (First Derivative)
Velocity measures the current rate of engagement growth per unit time (normalized to points/minute and comments/minute):

$$v_{\text{score}}(t) = \frac{\text{score}_t - \text{score}_{t-1}}{(t - t_{t-1}) / 60.0} \quad \left[\frac{\text{score points}}{\text{minute}}\right]$$

$$v_{\text{comment}}(t) = \frac{\text{comments}_t - \text{comments}_{t-1}}{(t - t_{t-1}) / 60.0} \quad \left[\frac{\text{comments}}{\text{minute}}\right]$$

### 2. Velocity Acceleration (Second Derivative)
Acceleration measures the rate of change in velocity. A positive acceleration indicates an **emerging narrative spurt** or breakout event:

$$a_{\text{score}}(t) = \frac{v_{\text{score}}(t) - v_{\text{score}}(t-1)}{(t - t_{t-1}) / 60.0} \quad \left[\frac{\text{score points}}{\text{minute}^2}\right]$$

$$a_{\text{comment}}(t) = \frac{v_{\text{comment}}(t) - v_{\text{comment}}(t-1)}{(t - t_{t-1}) / 60.0} \quad \left[\frac{\text{comments}}{\text{minute}^2}\right]$$

---

## Data Collection & Snapshot Architecture

Data ingestion is orchestrated by [`hn_collector.py`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/hn_collector.py):

### 1. Dual-Stream Output Design
- **`hn_events.jsonl` (Immutable Catalog)**: Appends each newly discovered story once upon initial detection (deduplicated by `post_id`).
- **`hn_snapshots.jsonl` (Time-Series Metric Log)**: Periodically records engagement metrics for all actively tracked stories. Each record captures the observation timestamp (`timestamp` / `__time`) alongside the original story publication time (`created_at`).

### 2. Active Pool Management (Sliding Window)
To prevent unbound memory and network consumption, the collector maintains an active pool of stories:
- **`ACTIVE_STORY_LIMIT`** (default: `100`): Maximum stories actively sampled per snapshot cycle.
- **`ACTIVE_WINDOW_HOURS`** (default: `12`): Stories older than 12 hours are automatically retired from active snapshotting.
- **Deduplication**: Loads existing IDs on startup to avoid re-cataloging across restarts.

### 3. Clean Hacker News URL Handling
External URLs are preserved when available (link submissions). For self/text posts (Ask HN, Show HN, Tell HN) or items without an external URL, clean fallback routing is applied:
$$\text{url} = \begin{cases} \text{item.url} & \text{if non-empty and valid} \\ \texttt{https://news.ycombinator.com/item?id=\{post\_id\}} & \text{otherwise} \end{cases}$$

---

## Apache Druid Setup & Ingestion

Apache Druid acts as the real-time OLAP store, ingesting newline-delimited JSON data and providing low-latency window aggregation over millions of rows.

### Ingestion Specifications
- [`druid_ingestion_spec.json`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/druid_ingestion_spec.json): Native batch indexer for `hn_events`.
- [`druid_snapshots_ingestion_spec.json`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/druid_snapshots_ingestion_spec.json): Native batch indexer for `hn_snapshots`.

### Ingesting Data into Druid:
```bash
# Ingest first-seen catalog
python submit_ingestion.py

# Ingest time-series snapshots
python submit_ingestion.py --snapshots
```
The submitter syncs data directly into the Druid MiddleManager Docker container volume (`/opt/shared/`) and monitors the indexing task to completion.

---

## Analytical SQL & Druid Queries

All production analytical queries are defined in [`queries.sql`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/queries.sql) and validated via [`test_queries.py`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/test_queries.py):

| Query | Focus | Mathematical Operation | Chart / Visualization Target |
|---|---|---|---|
| **Query 1** | Instantaneous Story Velocity | 1st Derivative: $\Delta \text{Engagement} / \Delta t$ | Table / Scatter Plot |
| **Query 2** | Velocity Acceleration | 2nd Derivative: $\Delta \text{Velocity} / \Delta t$ | Horizon Chart / Time-Series Line |
| **Query 3** | Real-Time Narrative Leaderboard | Latest snapshot ranked by $v_{\text{score}}$ and $a_{\text{score}}$ | Ranked Table / Alert Grid |
| **Query 4** | Macro Velocity Time-Series | 5-minute bucketed aggregations (`TIME_FLOOR`) | Macro Area / Trend Chart |
| **Query 5** | Breakout Surge Detection | Filtered on $a_{\text{score}} > 0$ and sorted by acceleration | Real-Time Surge Alert List |

---

## Apache Superset Dashboard

The visualization layer is automated via [`create_superset_dashboard.py`](file:///c:/Users/RITHIKESH%20S/InfoVelocity/create_superset_dashboard.py), creating a unified command center: **"InfoVelocity – Real-Time Narrative Intelligence"**.

### Datasets Utilized
1. **Physical Dataset**: `hn_snapshots` (direct Druid table)
2. **Virtual Dataset `hn_macro_velocity_5m`**: Pre-aggregated 5-minute velocity and author metrics.
3. **Virtual Dataset `hn_fastest_growing_stories`**: Real-time velocity ranking with sanitized HN URLs.
4. **Virtual Dataset `hn_breakout_accelerations`**: Stories experiencing positive acceleration bursts.

### Dashboard Layout & Hierarchy
```
+-----------------------------------------------------------------------------------------------+
|  ⚡ INFOVELOCITY: Real-Time Narrative Intelligence                                            |
|  [Header & Live Status Badges (🔴 LIVE | ⏱️ 10s REFRESH | 📡 HACKER NEWS API)]               |
+-----------------------------------------------------------------------------------------------+
|  ⚠️ VELOCITY ≠ VERACITY [Compact Information Banner & Expandable Kinematic Details]          |
+-----------------------------------------------------------------------------------------------+
|                                     ROW 1: KPI SUMMARY CARDS                                  |
| [Active Stories]     [Avg Score Velocity]     [Highest Score Velocity]    [Highest Acceleration]|
+-------------------------------------------------------+---------------------------------------+
| ROW 2: Engagement Velocity — 5-Minute Trend (~60%)    | ROW 2: Fastest Growing Narratives (~40%)|
| - Time-series Line Chart                              | - Instantaneous Velocity Ranking      |
| - Avg Score & Comment Velocity (Rate/min)             | - Direct Clickable Story Links        |
+-------------------------------------------------------+---------------------------------------+
| ROW 3: Breakout Monitor — Positive Acceleration (100% Full Width — Prominent Key Output)      |
| - Ranked Positive Surge Alert Table ($a > 0\text{ pts/min}^2$)                                |
| - Instantaneous 2nd Derivative Momentum Tracker                                               |
+-----------------------------------------------------------------------------------------------+
| ROW 4: Story Engagement Over Time (100% Full Width)                                           |
| - Comprehensive Historical Snapshot Log & Multi-Observation Progression Trajectory            |
+-----------------------------------------------------------------------------------------------+
```

---

## Project Directory & Inventory

```
InfoVelocity/
├── .env                              # Active environment configuration
├── .env.example                      # Configuration template
├── .gitignore                        # Git ignore patterns
├── README.md                         # Comprehensive project documentation
├── requirements.txt                  # Python dependencies
├── collector.log                     # Polling execution log
│
├── hn_collector.py                   # Hacker News data & snapshot collector
├── hn_events.jsonl                   # Immutable first-seen story catalog
├── hn_snapshots.jsonl                # Time-series engagement snapshot stream
│
├── druid_ingestion_spec.json         # Druid indexer spec: hn_events
├── druid_snapshots_ingestion_spec.json # Druid indexer spec: hn_snapshots
├── submit_ingestion.py               # Batch ingestion submitter & task poller
├── run_druid_query.py                # Ad-hoc Druid SQL query runner
│
├── queries.sql                       # 5 Production Velocity & Acceleration SQL queries
├── test_queries.py                   # Automated Druid SQL test suite
│
├── create_superset_dashboard.py      # Superset datasets, charts & dashboard provisioner
├── test_superset_dashboard.py        # Superset dashboard validation suite
│
├── demo_data_generator.py            # Reproducible demo data & kinematics generator
├── demo_hn_events.jsonl              # Synthetic demo story catalog
├── demo_hn_snapshots.jsonl           # Synthetic demo time-series snapshots
├── druid_demo_snapshots_ingestion_spec.json # Ingestion spec for demo datasource
│
├── legacy/                           # Archived pre-pivot prototype artifacts
│   ├── reddit_poller.py              # Deprecated Reddit PRAW poller
│   ├── reddit_events.jsonl           # Prototype Reddit event data
│   └── reddit_posts.jsonl            # Prototype Reddit post data
│
└── venv/                             # Python virtual environment
```

---

## Quickstart Guide & How to Run

### 1. Prerequisites
- Python 3.10+ (tested on Python 3.11)
- Docker & Docker Compose (optional, for Apache Druid & Apache Superset containers)
- No API keys required (Hacker News endpoints are public)

### 2. Environment Setup & Activation

First, clone the repository, set up the virtual environment, and install dependencies:

```bash
# Clone repository and enter directory
cd InfoVelocity

# Create virtual environment (if not already created)
python -m venv venv

# Activate virtual environment
# Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# Windows (Command Prompt):
.\venv\Scripts\activate.bat
# Linux / macOS (Bash/Zsh):
source venv/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

---

### 3. Step-by-Step Execution Commands

#### Option A: Instant Mathematical Demo & Kinematics Showcase (Recommended First Step)
Runs completely offline with zero configuration and prints out the verified first and second derivative momentum table in seconds:
```bash
python demo_data_generator.py
```
*Outputs: Generates `demo_hn_events.jsonl`, `demo_hn_snapshots.jsonl`, `druid_demo_snapshots_ingestion_spec.json`, and terminal verification tables.*

---

#### Option B: Live Hacker News Data Collection & Snapshotting
Fetches real-time submissions from the Hacker News Firebase REST API:

```bash
# 1. Quick test run: 1 cycle, fetch top 5 stories (exits immediately after completion)
python hn_collector.py --cycles 1 --limit 5

# 2. Continuous real-time collector (default: polls every 300s, tracks up to 100 stories):
python hn_collector.py

# 3. Fast offline simulation mode (e.g., 3 cycles with a 2-second interval):
python hn_collector.py --demo --cycles 3 --interval 2
```
*Outputs: Writes immutable first-seen stories to `hn_events.jsonl` and time-series observations to `hn_snapshots.jsonl`.*

---

#### Option C: Kinematics Processing (Instantaneous Velocity & Acceleration)
Processes snapshot observations in Python to compute 1st derivative ($v_{\text{score}}, v_{\text{comment}}$) and 2nd derivative ($a_{\text{score}}, a_{\text{comment}}$):
```bash
python compute_kinematics.py --input hn_snapshots.jsonl
```
*Outputs: Enriches `hn_snapshots.jsonl` with kinematic delta columns (`dt_sec`, `v_score`, `v_comment`, `a_score`, `a_comment`, `a_norm`, `rank_score`, `eligible`).*

---

#### Option D: Apache Druid Batch Ingestion (OLAP Store)
*Requires Apache Druid running on `http://localhost:8888`:*

```bash
# Ingest live time-series snapshots
python submit_ingestion.py --snapshots

# Or ingest the deterministic demo dataset into an isolated test datasource
python submit_ingestion.py --data demo_hn_snapshots.jsonl --spec druid_demo_snapshots_ingestion_spec.json
```

---

#### Option E: Query Validation & Superset Dashboard Automation
*Requires Apache Druid and Apache Superset running:*

```bash
# Run all 5 analytical SQL queries against Druid (Instantaneous velocity, acceleration, breakouts)
python test_queries.py

# Provision or validate Apache Superset charts, datasets, and dashboard
python test_superset_dashboard.py
```

---

#### Option F: Accessing the Web Dashboard & Website
Once the containers are running, access the web frontends in your browser:

1. **InfoVelocity Real-Time Superset Dashboard** (Main Website):
   - **URL**: [http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/](http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/)
   - **Default Login**: `admin` / `admin`
   - **Command to open in browser**:
     ```powershell
     # Windows (PowerShell)
     Start-Process "http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/"

     # Windows (Command Prompt)
     start http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/

     # Linux
     xdg-open "http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/"

     # macOS
     open "http://localhost:8088/superset/dashboard/infovelocity-narrative-tracker/"
     ```

2. **Apache Druid Web Console** (OLAP & SQL Query Interface):
   - **URL**: [http://localhost:8888](http://localhost:8888)
   - **Command to open in browser**:
     ```powershell
     Start-Process "http://localhost:8888"   # Windows PowerShell
     start http://localhost:8888             # Windows CMD
     ```

---

## Reproducible Demo Mode

For academic presentations, project evaluations, and offline demonstrations, live online discussions may not always produce a pronounced surge or breakout event on demand. InfoVelocity provides a **fully reproducible Demo Mode** with deterministic, mathematically structured narrative trajectories.

### Why Demo Mode Exists
- **Guaranteed Kinematic Showcase**: Demonstrates all 5 narrative archetypes (steady growth, exponential breakout, deceleration, controversy/comment surge, and flatline) with mathematically precise first and second derivatives.
- **Offline & Safe**: Completely decoupled from the live Hacker News API (`demo_hn_events.jsonl` and `demo_hn_snapshots.jsonl` do not touch production files).
- **Instantaneous Presentation Verification**: Displays calculated velocity and acceleration tables directly in the terminal and provides a dedicated Druid test spec (`druid_demo_snapshots_ingestion_spec.json`).

### 1. One-Command Live Visual Demo (Druid & Superset)
To immediately populate the Superset dashboard and Druid OLAP store with **only** the 5 fixed demo stories anchored to the current timestamp:
```powershell
python demo_data_generator.py --now --load-to-druid
```
*What this single command does:*
1. Generates the 5 deterministic story trajectories ending at the current time (`t5 = now`).
2. Computes the kinematic derivatives ($v_{\text{score}}, v_{\text{comment}}, a_{\text{score}}, a_{\text{comment}}, \text{rank\_score}$).
3. Clears old Druid segments and ingests the demo snapshots into `hn_snapshots`.
4. The Superset dashboard immediately displays the 5 fixed stories without any background noise!

---

### 2. Pure Terminal Demo (Offline Mathematical Verification)
If you just want to verify the mathematical kinematics table in your console without Docker:
```powershell
python demo_data_generator.py
```
This generates `demo_hn_events.jsonl`, `demo_hn_snapshots.jsonl`, and prints out the derivative calculus verification table.

### 3. Demonstrating Velocity & Acceleration Dynamics

| Story ID | Topic Archetype | Score Progression | Velocity ($v_{\text{score}}$) | Acceleration ($a_{\text{score}}$) | Expected UI / Analytical Behavior |
|---|---|---|---|---|---|
| **`90000002`** | **Rapidly Accelerating Breakout** | 5 → 15 → 45 → 125 → 275 → 525 | 2.0 → 50.0 pts/min | **+4.00 pts/min²** | **Top of Breakout Monitor & Leaderboard** |
| **`90000001`** | **Steady Normal Growth** | 20 → 35 → 50 → 65 → 80 → 95 | 3.0 pts/min (constant) | **0.00 pts/min²** | Constant slope in Trend Chart |
| **`90000003`** | **Decelerating Post** | 180 → 230 → 265 → 285 → 299 | 10.0 → 0.8 pts/min | **-0.60 pts/min²** | High total score but absent from Breakouts |
| **`90000004`** | **Controversial Debate** | Comments: 8 → 28 → 68 → 368 | $v_{\text{comm}} = 26.0$ comm/min | **+1.20 comm/min²** | Divergence between Score & Comment Velocity |
| **`90000005`** | **Inactive Flatline** | 1 → 2 → 2 → 2 → 3 → 3 | $\approx 0.0$ pts/min | **0.00 pts/min²** | Bottom baseline |

---

## How to Read the Dashboard

The **InfoVelocity** dashboard translates physics-based kinematics into real-time narrative intelligence. Below is a guide on how to interpret each component:

### 1. Header & Executive KPI Summary Cards
- **Header Live Badges**: Indicates active 10s auto-refresh cadence, Hacker News API connection status, and local IST timestamp of last update.
- **Epistemic Disclaimer ("Velocity ≠ Veracity")**: Highlights that rapid virality and discussion growth represent information diffusion momentum across the social graph, not factual truth or credibility.
- **Active Stories**: Total distinct discussion threads observed over the last 6-hour lookback window, accompanied by a 5-minute moving sparkline trend.
- **Average Score Velocity**: Mean velocity across all active discussions in points/min.
- **Highest Score Velocity**: The single fastest gaining story by instantaneous upvote velocity.
- **Current Top Acceleration**: The highest positive surge acceleration ($a_{\text{score}} > 0$) observed in the latest snapshot batch, spotlighting immediate breakouts.
- **Peak Acceleration (last 6h)**: The absolute maximum surge rate detected across the entire 6-hour observation window (sane capped at 50 pts/min²).

### 2. Analytical Charts & Movement Maps
- **Engagement Velocity — 5-Minute Trend**:
  - Dual time-series chart plotting average **Score Velocity** (blue line) and **Comment Velocity** (cyan/green line) aggregated into 5-minute buckets.
  - Highlights whether platform attention is shifting towards upvoting (agreement/curation) or commenting (discussion/debate).
- **Momentum Map (Kinematic Scatter Plot)**:
  - 4-quadrant interactive bubble chart where **X-axis = Score Velocity**, **Y-axis = Score Acceleration**, and **Bubble Size = Score**.
  - **Quadrants**:
    - **Breakout (Upper-Right)**: $v > 0, a > 0$ — Stories experiencing accelerating virality. Early detection zone for major breaking stories.
    - **Cooling (Lower-Right)**: $v > 0, a < 0$ — High velocity but negative acceleration. Popular stories that are maturing and tapering off.
    - **Steady (Upper-Left/Center)**: $v > 0, a \approx 0$ — Constant, linear community interest.
    - **Fading / Dormant (Lower-Left)**: $v \le 0, a \le 0$ — Inactive discussions dropping off the front page.

### 3. Ranked Data Tables & Audit Logs
- **Fastest Growing Narratives (Leaderboard)**:
  - Ranked by weighted velocity rank ($\text{rank\_score} = v_{\text{score}} \cdot \ln(1 + \text{score})$), which balances high velocity against total community validation.
  - Features clickable story titles linking directly to the source article (or HN thread fallback).
  - Post IDs are hidden to maximize screen space; headers are frozen for seamless scrolling.
- **Breakout Monitor — Positive Acceleration**:
  - Filtered exclusively for narratives with $a_{\text{score}} > 0$ and ranked by normalized acceleration ($a_{\text{norm}} = a / \text{score}$).
  - Cell backgrounds are conditionally formatted with rich green highlights for acceleration surges.
- **Story Engagement Over Time (Full Trajectory Audit)**:
  - Comprehensive snapshot log sorted latest-first (**Observation Time DESC**) displaying local IST timestamps with relative age (e.g., `22:41:46 (2m ago)`) and story publication age (e.g., `1.5h`).
  - Native filters on **Story Title** and **Post ID** allow users to isolate and trace the complete historical trajectory of any given story across consecutive observation rounds.

---

## Screenshot Checklist & Verification

When reviewing or presenting the dashboard UI at 1080p resolution, verify the following elements:

- [ ] **Header Emojis & Badges**: All emojis (`⚡ 🔴 ⏱️ 📡`) render cleanly in UTF-8 without question mark (`???`) artifacts.
- [ ] **Disclaimer Banner**: Expandable epistemic notice (`Velocity ≠ Veracity`) with details dropdown.
- [ ] **5 KPI Cards**: Row 1 contains 5 cards aligned with equal heights (Active Stories, Avg Velocity, Peak Velocity, Current Top Accel, Peak Accel (6h)).
- [ ] **Sparkline Trendlines**: Active Stories and Velocity cards display mini trendlines without errors.
- [ ] **Macro Trend**: ECharts time-series line chart plots 5-minute velocity moving windows without negative dips.
- [ ] **Momentum Map**: Bubble chart plots stories across 4 quadrants with legend color coding and bubble size scaling with score.
- [ ] **Leaderboard Table**: Clickable title links, hidden Post ID, frozen header, no horizontal scrolling needed at 1080p, row limit 100 (no yellow warning).
- [ ] **Breakout Monitor**: Displays positive surges with green conditional formatting, row limit 100 (no yellow warning).
- [ ] **Audit Table**: Chronologically sorted Observation Time DESC, relative timestamps (`2m ago`), story age (`1.2h`), row limit 500 (no yellow warning).
- [ ] **Native Filter Panel**: Left filter panel includes Time Window, Min Score, Author, Story Title, and Post ID.
- [ ] **Zero Data Errors**: All charts return HTTP 200 with zero planning/SQL errors.

---

## Limitations & Future Roadmap

### Current Limitations
- **Polling Cadence**: Metric observations are recorded at discrete polling intervals (default: 300s), bounding real-time resolution to the poll frequency.
- **Single Source Ingestion**: Currently monitors Hacker News exclusively.
- **Sliding Window Retention**: Ingestion focuses on the active 12-hour publication window to optimize OLAP storage.

### Future Roadmap (Next Phases)
- **Streaming Ingestion**: Transition from batch snapshot files to an event streaming broker for sub-second ingestion latency.
- **Cross-Platform Adapters**: Add modular ingestion connectors for open discussion platforms (e.g., Bluesky, Mastodon, Lemmy) while preserving the shared velocity calculus.
- **Automated Webhook Alerts**: Implement webhook notifications triggered when $a_{\text{score}}$ exceeds configurable standard deviation thresholds.




