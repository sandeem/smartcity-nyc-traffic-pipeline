# SmartCity NYC - Real-Time Traffic Intelligence Pipeline

A production-grade, end-to-end streaming data pipeline that ingests live traffic data from the **TomTom API**, enriches it with **1.8M OpenStreetMap road segments** using geospatial H3 indexing, and delivers real-time congestion analytics through a **Lakeview dashboard** and **autonomous Slack alerting**.

Built entirely on **Databricks** using the **Medallion Architecture** (Bronze -> Silver -> Gold) with Delta Lake, Structured Streaming, and Unity Catalog.

---

## Architecture

```
TomTom API --> S3 Landing Zone (JSON) --> Auto Loader (cloudFiles) --> Bronze (Raw JSON)
                                                                          |
              OSM Roads (1.8M rows) --> H3 Geospatial Indexing            |
                                              |                           |
                                    +---------v---------------------------v--------+
                                    |              Silver Layer                    |
                                    |    Broadcast Join + Quarantine Logic         |
                                    +---------------------+------------------------+
                                                          |
                                                 +--------v--------+
                                                 |   Gold Layer    |
                                                 |  CI Calculation |
                                                 |  1-min Windows  |
                                                 |  MERGE Upsert   |
                                                 +---+--------+----+
                                                     |        |
                                          +----------v-+  +---v--------------+
                                          |  Lakeview   |  |  Slack Alerts   |
                                          |  Dashboard  |  | (GRIDLOCK only) |
                                          +-------------+  +-----------------+
```

---

## Key Features

| Feature | Description |
|---|---|
| **Real-Time Ingestion** | TomTom API polled every 10 minutes via scheduled Databricks job |
| **Geospatial Enrichment** | 1.8M OSM road segments indexed with Uber H3 hexagons (Resolution 10, ~15m precision) |
| **Data Quality** | Quarantine pipeline routes bad rows to `bronze_errors` with categorized error reasons |
| **Agentic Alerting** | Autonomous Slack notifications for GRIDLOCK events with recency (30 min) and cooldown (30 min/road) guards |
| **MERGE Upsert** | Prevents duplicate Gold rows from checkpoint resets via `foreachBatch` + Delta MERGE |
| **Secrets Management** | All credentials (AWS, API keys, S3 paths, webhooks) stored in Databricks Secrets -- zero hardcoded values |

---

## Congestion Index Formula

```
Congestion Index (CI) = 1 - (currentSpeed / freeFlowSpeed)
```

| Status | CI Range | What It Means |
|---|---|---|
| **CLEAR** | CI <= 0.40 | Normal traffic flow (16+ mph) |
| **HEAVY** | 0.40 < CI <= 0.80 | Significant delays (7-16 mph) |
| **GRIDLOCK** | CI > 0.80 | Near standstill (4-5 mph) |

---

## Pipeline Notebooks

### Part A - Pull Real-Time Traffic Data
**`Bronze_TomTom_STREAM_Part_A_pull_data`**
- Uses `boto3.client("s3")` with credentials from Databricks Secrets (`aws-creds` scope) for serverless-compatible S3 writes
- Calls TomTom Flow Segment API for GPS point near City Hall / Brooklyn Bridge (40.7128, -74.0060)
- API key secured via Databricks Secrets (`tomtom-keys/api-key`)
- S3 landing path retrieved from Databricks Secrets (`smartcity-secrets/s3-path`)
- Writes each JSON response with unique timestamped filename

### Part B - Stream to Bronze
**`Bronze_TomTom_STREAM_Part_B_stream_to_bronze`**
- Passes AWS credentials as reader-scoped `.option("fs.s3a.*")` calls from Databricks Secrets (`aws-creds` scope) — serverless-compatible
- Auto Loader (`cloudFiles`) incrementally detects new JSON files in S3
- S3 source path retrieved from Databricks Secrets (`smartcity-secrets/s3-path`)
- Schema auto-inferred on first run, stored at checkpoint path
- Writes to Unity Catalog managed Delta table (`bronze_traffic`)

### Part C - Enrich to Silver
**`Silver_Traffic_STREAM_Part_C_enrich_to_silver`**
- **Geospatial Indexing**: Converts 1.8M OSM road geometries (WKT) to H3 hex IDs using `ST_Centroid` + `h3_longlatash3`
- **Road Crosswalk**: Builds a 58-row bridge table mapping traffic H3 IDs to OSM road names via Broadcast Hash Join
- **Enrichment Pipeline**: Structured Streaming with watermarking, LEFT JOIN to crosswalk, and quarantine routing
- **Coalesce Fallback**: Handles two bronze data formats (old nested JSON vs. new top-level) without dropping rows
- **Timestamp Preservation**: Uses original `pulled_at` instead of `current_timestamp()` to prevent temporal dilution

### Part D - Aggregate to Gold
**`Gold_Traffic_STREAM_Part_D_aggregate_to_gold`**
- Calculates Congestion Index with negative-value capping (speeding cars -> CI = 0)
- Aggregates into 1-minute sliding windows per road
- MERGE upsert via `foreachBatch` prevents duplicate windows
- **Agentic Slack Alerting**: Webhook URL retrieved from Databricks Secrets (`smartcity-secrets/slack-webhook`) with:
  - Recency guard (skip events older than 30 min)
  - Cooldown guard (max 1 alert per road per 30 min)
  - Prevents spam from checkpoint resets reprocessing historical data

---

## Performance Optimizations

| Optimization | Where | Impact |
|---|---|---|
| **Broadcast Hash Join** | Silver crosswalk join (58 rows) | 1.16x faster - eliminates network shuffle |
| **Adaptive Query Execution (AQE)** | Gold aggregation | **4.37x faster** (77% improvement) |
| **Z-ORDER** | `silver_road_crosswalk` by `h3_id` | 1.24x faster lookups via data skipping |
| **Liquid Clustering** | `silver_osm_enriched` by `h3_id` | Auto-compacts on write, no manual OPTIMIZE |
| **OPTIMIZE** | `gold_traffic_stats` | 29 files -> 1 file (96.6% fewer, 79.6% smaller) |
| **Watermarking** | Silver + Gold streams | Bounded memory for late-arriving data |
| **`trigger(availableNow=True)`** | All streams | Job-safe batch processing (vs. continuous) |

---

## Data Tables

| Table | Layer | Rows | Description |
|---|---|---|---|
| `bronze_traffic` | Bronze | ~332 | Raw TomTom JSON responses |
| `bronze_osm_roads` | Bronze | 1.8M | Static OpenStreetMap road segments |
| `silver_osm_enriched` | Silver | 1,825,656 | H3-indexed OSM roads |
| `silver_road_crosswalk` | Silver | 58 | H3 -> road name bridge table |
| `silver_traffic` | Silver | ~140K | Enriched traffic with road names |
| `bronze_errors` | Quarantine | ~2,484 | Failed data quality rows |
| `gold_traffic_stats` | Gold | ~1,400 | 1-min windowed congestion aggregates |

All tables live in Unity Catalog: `bootcamp_students.zachy_sandeepmanoharan1.*`

---

## Dashboard

### Lakeview Dashboard - "SmartCity NYC Traffic Monitor"

**4 content pages + Global Filters | 15 datasets | 28 widgets**

| Page | Purpose |
|---|---|
| **Traffic Overview** | Presentation mode - all-time KPIs (164K+ observations, 14 GRIDLOCKs, peak CI 0.84), congestion trend, road comparisons, daily collection summary |
| **Live Monitor** | Demo mode - 24h KPIs with period-over-period comparison, Active Gridlocks counter (red/green), real-time road status table |
| **Operations** | Pipeline health - congestion by street (Brooklyn Bridge area), road x hour heatmap, hourly patterns, freshness counter (green/orange/red), GRIDLOCK alert history |
| **Historical Analysis** | Day-of-week congestion bar chart, road x day heatmap - Saturday worst (0.23 avg CI), Monday best (0.0) |
| **Global Filters** | Road, Traffic Status, and Date Range filters applied across all pages |
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/1-Traffic%20Overview%20(Top%20Half).png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 1: Traffic Overview (Top Half)</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/2-Traffic%20Overview%20(Bottom%20Half).png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 2: Traffic Overview (Bottom Half)</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/3-Operations%20Page%20(Top%20Half).png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 3: Operations Page (Top Half)</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/4-Operations%20Page%20(Bottom%20Half).png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 4: Operations Page (Bottom Half)</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/6-Historical%20Traffic%20Analysis.png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 5: Historical Traffic Analysis</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/7-Live%20Monitoring%20Dashboard.png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 6: Live Monitoring Dashboard</i>
</p>
 
<br><p align="center">
<img src="https://github.com/sandeem/smartcity-nyc-traffic-pipeline/blob/main/Dashboard%20Photos/9-Slack%20Notification%20Page.png" width="900" alt="Traffic Dashboard">
</p>
<p align="center">
<i>Figure 7: Slack Notifications page</i>
</p>

---

## Secrets Management

All credentials are stored in **Databricks Secrets** -- zero hardcoded values in any notebook.

| Scope | Key | Used In | Purpose |
|---|---|---|---|
| `aws-creds` | `access-key` | Part A, Part B | AWS S3 access key for reading/writing to landing zone |
| `aws-creds` | `secret-key` | Part A, Part B | AWS S3 secret key for reading/writing to landing zone |
| `tomtom-keys` | `api-key` | Part A | TomTom Flow Segment API authentication |
| `smartcity-secrets` | `s3-path` | Part A, Part B | S3 landing zone path (bucket + prefix) |
| `smartcity-secrets` | `slack-webhook` | Part D | Slack incoming webhook URL for GRIDLOCK alerts |

---

## Job Configuration

| Setting | Value |
|---|---|
| **Job Name** | `SmartCity-RT_Pipeline` |
| **Schedule** | Every 10 minutes (`0 0/10 * * * ?`), America/New_York |
| **Task Order** | Part A -> Part B -> Part C -> Part D (sequential) |
| **Compute** | Serverless |
| **Alerting** | Slack webhook for GRIDLOCK events |

---

## Future Enhancements

The current pipeline monitors a **single GPS point** near City Hall / Brooklyn Bridge. Because TomTom returns one speed reading per point, all 7 nearby roads share the same Congestion Index. The pipeline architecture already supports **multi-point monitoring** with minimal changes:

- **Only Part A needs modification** - replace the single `POINT` variable with a dictionary of named GPS coordinates. Parts B, C, and D handle the additional data automatically.
- **Candidate locations:** Manhattan Bridge, Times Square, Holland Tunnel, FDR Drive @ 42nd St, BQE @ Atlantic Ave, 34th & 5th Ave, Williamsburg Bridge
- **API budget:** 8 points x 6 pulls/hour x 24h = 1,152 calls/day (TomTom free tier allows ~2,500/day)
- **Expected outcome:** Independent congestion values per location, revealing location-specific patterns (e.g., Times Square peaks at 6pm vs Holland Tunnel at 5pm)

---

## Setup & Reproduction

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- AWS account with S3 bucket for landing zone
- TomTom API key ([get one free](https://developer.tomtom.com/))
- Slack webhook URL for alerting

### Step 1: Store all credentials in Databricks Secrets

```bash
# AWS S3 credentials
databricks secrets create-scope aws-creds
databricks secrets put-secret aws-creds access-key --string-value "YOUR_AWS_ACCESS_KEY"
databricks secrets put-secret aws-creds secret-key --string-value "YOUR_AWS_SECRET_KEY"

# TomTom API key
databricks secrets create-scope tomtom-keys
databricks secrets put-secret tomtom-keys api-key --string-value "YOUR_TOMTOM_KEY"

# S3 path and Slack webhook
databricks secrets create-scope smartcity-secrets
databricks secrets put-secret smartcity-secrets s3-path --string-value "s3a://YOUR_BUCKET/path/to/raw_data/"
databricks secrets put-secret smartcity-secrets slack-webhook --string-value "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### Step 2: Upload OSM data
Upload NYC road segments to `bronze_osm_roads` table (1.8M rows with `geometry_wkt` column)

### Step 3: Run notebooks in order
- Part A: Pulls live data to S3
- Part B: Streams S3 -> Bronze
- Part C: Enriches with geospatial joins -> Silver
- Part D: Aggregates + alerts -> Gold

### Step 4: Schedule the job for continuous monitoring

---

## Tech Stack

| Component | Technology |
|---|---|
| **Platform** | Databricks on AWS |
| **Storage** | Delta Lake + Unity Catalog |
| **Streaming** | Spark Structured Streaming + Auto Loader |
| **Geospatial** | Uber H3 (Resolution 10) + Sedona spatial SQL |
| **API** | TomTom Traffic Flow Segment v4 |
| **Alerting** | Slack Webhooks (autonomous/agentic) |
| **Secrets** | Databricks Secrets (3 scopes, 5 keys) |
| **Visualization** | Databricks Lakeview Dashboard |
| **Language** | Python (PySpark) + SQL |

---

## Project Structure

```
capstone_project/
|-- Bronze_TomTom_STREAM_Part_A_pull_data.ipynb        # API ingestion
|-- Bronze_TomTom_STREAM_Part_B_stream_to_bronze.ipynb  # Auto Loader -> Bronze
|-- Silver_Traffic_STREAM_Part_C_enrich_to_silver.ipynb  # Geospatial enrichment -> Silver
|-- Gold_Traffic_STREAM_Part_D_aggregate_to_gold.ipynb   # Aggregation + alerting -> Gold
|-- SmartCity Project Reference Guide.ipynb              # Detailed project documentation
|-- Presentation Speaker Notes.md                        # 10-min presentation script
|-- smartcity_architecture.png                           # Pipeline architecture diagram
|-- README.md                                            # This file
```

---

## Author

**Sandeep Manoharan**
Built as a capstone project for the DataExpert.io Data Engineering Bootcamp.

---

## License

This project is for educational purposes as part of the DataExpert.io bootcamp curriculum.
