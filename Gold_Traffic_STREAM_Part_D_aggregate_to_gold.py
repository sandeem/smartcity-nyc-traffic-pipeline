# Databricks notebook source
# MAGIC %md
# MAGIC # Part D — Aggregate to Gold and Trigger Alerts
# MAGIC
# MAGIC **Pipeline Stage**: `silver_traffic` → `gold_traffic_stats` + Slack Alerts
# MAGIC
# MAGIC **What this notebook does**: Reads the enriched Silver stream, calculates the Congestion Index (`CI = 1 - currentSpeed/freeFlowSpeed`), aggregates into 1-minute sliding windows per road, classifies traffic status (GRIDLOCK/HEAVY/CLEAR), MERGE-upserts into Gold, and sends Slack alerts for GRIDLOCK events.
# MAGIC
# MAGIC | Detail | Value |
# MAGIC |---|---|
# MAGIC | **Input** | `silver_traffic` (enriched speed data with road names) |
# MAGIC | **Output** | `gold_traffic_stats` (windowed aggregates) + Slack webhook alerts |
# MAGIC | **Secrets** | `smartcity-secrets/slack-webhook` (Slack alerting endpoint) |
# MAGIC | **Optimizations** | AQE, auto shuffle partitions, MERGE upsert (vs append) |
# MAGIC | **Agentic Action** | Slack GRIDLOCK alerts with 30-minute recency + cooldown guards |

# COMMAND ----------

# DBTITLE 1,Setup Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Setup: Alert Cooldowns and Checkpoint
# MAGIC Initializes the in-memory alert cooldown dictionary (prevents Slack spam) and optionally resets the Gold checkpoint. The cooldown dict persists in cluster memory for the duration of the job run.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Alert Cooldown Dictionary**: A Python `dict` stored in cluster memory (`{road_name: last_alert_time}`) that tracks when each road was last alerted. Prevents the same road from triggering multiple Slack messages within 30 minutes.
# MAGIC - **Checkpoint Reset**: Deleting the Gold checkpoint forces Spark to reprocess all Silver data from scratch. This is necessary when the aggregation logic changes (e.g., new window size, new CI formula) but causes duplicate processing of historical data.

# COMMAND ----------

# DBTITLE 1,Initialize Alert Cooldowns
from datetime import datetime, timedelta

# This dictionary lives in the cluster's memory while the job runs
# Key: road_name, Value: last_alert_time
alert_cooldowns = {}

# COMMAND ----------

# DBTITLE 1,Reset Gold Checkpoint
# We maintain persistent checkpoints for Bronze and Silver to ensure Exactly-Once processing and reduce compute costs. 
# Gold checkpoints are only reset during logic deployments to trigger full historical re-calculations.

# COMMENTED OUT FOR PRODUCTION: Uncomment only when deploying logic changes
# dbutils.fs.rm("/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/_checkpoints/gold_traffic", True)

# COMMAND ----------

# DBTITLE 1,Schema Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Schema: Create Gold Table
# MAGIC Creates the `gold_traffic_stats` Delta table if it doesn’t exist. This is idempotent (`IF NOT EXISTS`) and safe to run on every execution.
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `road_name` | STRING | Road name from OSM crosswalk |
# MAGIC | `window_start` / `window_end` | TIMESTAMP | 1-minute aggregation window boundaries |
# MAGIC | `avg_congestion_index` | DOUBLE | Mean CI for all observations in window |
# MAGIC | `traffic_status` | STRING | GRIDLOCK (>0.8) / HEAVY (>0.4) / CLEAR (≤0.4) |
# MAGIC | `total_observations` | INT | Number of API pings in window |
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Idempotent**: An operation that produces the same result whether run once or many times. `CREATE TABLE IF NOT EXISTS` will succeed on the first run and silently no-op on subsequent runs.
# MAGIC - **Delta Table** (`USING DELTA`): The open-source storage format used throughout this pipeline. Adds ACID transactions, schema enforcement, and time travel to Parquet files.

# COMMAND ----------

# DBTITLE 1,Create Gold Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats (
# MAGIC   road_name STRING,
# MAGIC   window_start TIMESTAMP,
# MAGIC   window_end TIMESTAMP,
# MAGIC   avg_congestion_index DOUBLE,
# MAGIC   traffic_status STRING,
# MAGIC   total_observations INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Congestion Math and Windowing
# MAGIC
# MAGIC This script calculates the Congestion Index and groups traffic data into 1-minute sliding windows to create a real-time “moving average” for every road.
# MAGIC
# MAGIC **Core formula:**
# MAGIC ```
# MAGIC Congestion Index (CI) = 1 - (currentSpeed / freeFlowSpeed)
# MAGIC ```
# MAGIC
# MAGIC - `currentSpeed` — How fast cars are actually moving right now on that road
# MAGIC - `freeFlowSpeed` — How fast cars usually move when the road is completely empty (no traffic)
# MAGIC - CI = 0.0 → Free-flowing traffic | CI = 0.84 → Near standstill (GRIDLOCK)
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Sliding Window** (`window(col, "1 minute", "1 minute")`): A time-based grouping that aggregates all observations within a fixed-size window. With a 1-minute window and 1-minute slide, each window captures exactly 1 minute of data. Overlapping windows (e.g., 5-min window, 1-min slide) create a smoothing effect.
# MAGIC - **Watermarking** (`.withWatermark("ingest_time", "1 second")`): Tells Spark how long to keep old aggregation state in memory. Data arriving later than the watermark threshold is dropped. Here, a minimal watermark is used since data is processed in batch mode.
# MAGIC - **Traffic Status Classification**: Business logic that maps CI ranges to human-readable labels: **GRIDLOCK** (CI > 0.80, near standstill at 4-5 mph), **HEAVY** (CI > 0.40, significant delays at 7-16 mph), **CLEAR** (CI ≤ 0.40, normal flow).

# COMMAND ----------

# DBTITLE 1,Congestion Index Calculation and Windowing
from pyspark.sql.functions import *

# 1. READ FROM SILVER
# Connects to the enriched Silver table as a continuous stream of data.
silver_df = spark.readStream.table("bootcamp_students.zachy_sandeepmanoharan1.silver_traffic")

# .withWatermark: Standardizes how long Spark waits for late data (15 mins) before dropping it from memory. 
# It takes less than 6 mins to load the data to Silver tables, 2–5 minute API/network lag, and 3 minutes of processing overhead, ensuring data isn't dropped if total latency exceeds 15 minutes.
# .withColumn("ci_raw"): Implements the core formula: CI = 1 - (Current / FreeFlow).
# .withColumn("congestion_index"): Data cleaning logic—if cars go faster than the speed limit (CI < 0), we cap it at 0.
# If a car is "speeding" (going faster than the Free Flow speed), the math 1 - (current / free_flow) will result in a negative number (e.g., 1-1.2 = -0.2). Since we cannot have "negative congestion," this part of the code catches those negative values and forces them to be 0 (meaning "Perfectly Clear/No Traffic").

# 2. APPLY MATH & WATERMARKING
# .withWatermark: Waits 20 minutes for late data.
# This covers 6m ingest, 5m API lag, and 9m processing/windowing overhead.
# .withWatermark("ingest_time", "20 minutes")
# .withWatermark("ingest_time", "1 minute")
gold_logic = silver_df \
    .withWatermark("ingest_time", "1 second") \
    .withColumn("ci_raw", (1 - (col("currentSpeed") / col("freeFlowSpeed")))) \
    .withColumn("congestion_index", when(col("ci_raw") < 0, 0).otherwise(col("ci_raw")))

# 3. AGGREGATE BY WINDOW AND ROAD
# Sliding Windows - We use a 5-minute window to smooth out temporary stop-and-go noise, and a 2-minute slide to ensure our dashboard updates frequently enough to be considered 'Real-Time'.
# Example - Window 1: 12:00 to 12:10, Window 2: 12:01 to 12:11, etc.
# This creates a "moving average" effect to smooth out sudden traffic spikes on the dashboard
gold_agg = gold_logic.groupBy(
    # window(col("ingest_time"), "10 minutes", "1 minute"), 
    window(col("ingest_time"), "1 minute", "1 minute"), 
    col("full_road_name").alias("road_name")
).agg(
    # Aggregates the average congestion score and counts how many API pings happened in that window
    avg("congestion_index").alias("avg_congestion_index"),
    count("*").alias("total_observations")
).withColumn("traffic_status", 
    # Business Logic - Categorizes the road state based on the congestion percentage
    # > 80% congestion = GRIDLOCK | > 40% = HEAVY | else = CLEAR
    when(col("avg_congestion_index") > 0.8, "GRIDLOCK")
    .when(col("avg_congestion_index") > 0.4, "HEAVY")
    .otherwise("CLEAR")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Merge (Upsert) Operation with Agentic Alerting
# MAGIC
# MAGIC This script uses a MERGE (Upsert) operation via `foreachBatch` to sync data into the final Gold table, then checks for GRIDLOCK events and sends Slack alerts with recency and cooldown guards.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **MERGE (Upsert)**: A Delta Lake SQL operation that combines INSERT and UPDATE in one atomic statement. If a row with the same `(road_name, window_start)` already exists, it’s updated; otherwise, a new row is inserted. This prevents duplicate windows from checkpoint resets.
# MAGIC - **`foreachBatch`**: A Spark Structured Streaming output mode that hands each micro-batch to a custom Python function. This allows running arbitrary logic (like MERGE + alerting) on each batch, instead of just appending.
# MAGIC - **Agentic Action**: An autonomous, event-driven response triggered by data conditions — in this case, the pipeline detects GRIDLOCK status and sends a Slack notification without human intervention.
# MAGIC - **Recency Guard**: Checks if the GRIDLOCK event’s `window_start` is within the last 30 minutes. Prevents old GRIDLOCK events (e.g., from Mar 21) from re-triggering alerts when the checkpoint is reset and historical data is reprocessed.
# MAGIC - **Cooldown Guard**: Limits alerts to once per road per 30 minutes using the `alert_cooldowns` dictionary. Prevents Slack spam when multiple batches contain the same GRIDLOCK event.
# MAGIC - **Slack Webhook**: A URL provided by Slack that accepts HTTP POST requests with a JSON payload and posts the message to a configured channel. No Slack SDK needed — just `requests.post()`.

# COMMAND ----------

# DBTITLE 1,MERGE Upsert with Agentic Slack Alerting
import requests
import json

# Retrieve webhook URL from secrets (once, outside the function)
SLACK_WEBHOOK_URL = dbutils.secrets.get(scope="smartcity-secrets", key="slack-webhook")

# ── AGENTIC ACTION: Slack notification for GRIDLOCK alerts ──
def notify_gridlock(road, ci):
    payload = {"text": f"\U0001f6a8 *GRIDLOCK ALERT* \U0001f6a8\n\U0001f4cd Road: {road}\n\U0001f4c9 Congestion Index: {ci:.2f}"}
    requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload))

# ── MERGE + AGENTIC ALERTING (combined function) ──
# This runs for every micro-batch the stream produces.
# 1. MERGE upserts data into Gold
# 2. Checks for RECENT GRIDLOCK rows and sends Slack alerts with a 30-minute cooldown per road
def merge_to_gold(batch_df, batch_id):
    # 1. Register the batch as a temporary table so SQL can read it
    batch_df.createOrReplaceTempView("updates")
    
    # 2. Run the SQL MERGE (upsert) — updates existing windows, inserts new ones
    batch_df.sparkSession.sql("""
        MERGE INTO bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats t
        USING (SELECT *, window.start as window_start, window.end as window_end FROM updates) s
        ON t.road_name = s.road_name AND t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # 3. AGENTIC ACTION: Alert on GRIDLOCK with recency + cooldown guards
    #    - RECENCY CHECK: Only alert if the traffic window is within the last 30 minutes.
    #      This prevents old GRIDLOCK events (e.g., Mar 21) from re-triggering alerts
    #      when the Gold checkpoint is reset and all historical data is reprocessed.
    #    - COOLDOWN CHECK: Only alert once per road per 30 minutes to avoid spam.
    #      alert_cooldowns dict is defined in cell 2 and persists in cluster memory.
    gridlock_rows = batch_df.filter("traffic_status = 'GRIDLOCK'").collect()
    current_time = datetime.now()

    for row in gridlock_rows:
        road = row['road_name']
        ci = row['avg_congestion_index']
        window_time = row['window']['start']
        
        # RECENCY CHECK: How old is this GRIDLOCK event?
        # If the event happened more than 30 minutes ago, it's stale historical data
        # (e.g., from a checkpoint reset reprocessing old Mar 21 data) — skip it
        age_minutes = (current_time - window_time).total_seconds() / 60
        if age_minutes > 30:
            print(f"\u23ed Skipping old GRIDLOCK for {road} (CI: {ci:.2f}) — event is {int(age_minutes)}m old")
            continue
        
        # COOLDOWN CHECK: Look up when we last alerted for this specific road
        # Returns None if we've never alerted this road before
        last_alert = alert_cooldowns.get(road)
        
        # If we've NEVER alerted this road (last_alert is None)
        # OR it's been more than 30 minutes since the last alert for this road
        # → then it's safe to send a new alert without spamming
        if last_alert is None or (current_time - last_alert) > timedelta(minutes=30):
            # SEND the Slack alert with road name and congestion score
            notify_gridlock(road, ci)
            # SAVE the current time so we know when we last alerted this road
            # This prevents the same road from triggering another alert for 30 minutes
            alert_cooldowns[road] = current_time
            print(f"\U0001f6a8 Alert sent for {road} (CI: {ci:.2f}) at {window_time}")
        else:
            # We already alerted this road recently — calculate how many minutes
            # are left before the cooldown expires and we can alert again
            remaining = 30 - int((current_time - last_alert).total_seconds() / 60)
            print(f"\u23f3 Skipping alert for {road} — cooldown ({remaining}m remaining)")

# ── STREAM START ──
# Checkpoint: Remembers exactly what data has been moved to Gold to handle restarts.
checkpoint_gold = "/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/_checkpoints/gold_traffic"

# .foreachBatch(merge_to_gold): Hands each micro-batch to our combined MERGE + Alert function
# .trigger(availableNow=True): Processes all available data then stops (job-safe)
query = (gold_agg.writeStream
    .foreachBatch(merge_to_gold)
    .option("checkpointLocation", checkpoint_gold)
    .outputMode("update")
    .trigger(availableNow=True)
    .start())

# JOB MODE FIX: Block until stream completes so the job doesn't kill the cluster mid-processing
query.awaitTermination()
print("Gold stream finished. gold_traffic_stats is up to date.")

# COMMAND ----------

# DBTITLE 1,Analysis Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Analysis: Query Gold Data
# MAGIC Optional cells to inspect GRIDLOCK/HEAVY events, view recent aggregation windows, and track Brooklyn Bridge congestion trends. These are **not required** for the pipeline job but useful for monitoring and debugging.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **`from_utc_timestamp(col, 'America/New_York')`**: Converts a UTC timestamp to Eastern Time (automatically handling EST/EDT daylight saving). All timestamps in the Gold table are stored in UTC for consistency.
# MAGIC - **Gold Layer**: The final layer of the Medallion Architecture. Contains business-level aggregates ready for dashboards, reports, and alerting. Data is clean, deduplicated, and optimized for query performance.

# COMMAND ----------

# DBTITLE 1,Query GRIDLOCK and HEAVY Events
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   from_utc_timestamp(window_start, 'America/New_York') AS window_start_local, 
# MAGIC   road_name, 
# MAGIC   ROUND(avg_congestion_index, 4) AS avg_congestion_index, 
# MAGIC   traffic_status,
# MAGIC   total_observations
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
# MAGIC WHERE traffic_status IN ('GRIDLOCK', 'HEAVY')
# MAGIC ORDER BY window_start DESC;

# COMMAND ----------

# DBTITLE 1,Recent Gold Windows
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   window_start, 
# MAGIC   window_end, 
# MAGIC   road_name, 
# MAGIC   avg_congestion_index, 
# MAGIC   total_observations
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
# MAGIC -- WHERE road_name = 'Brooklyn Bridge' -- Pick one road to see the "slide"
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Brooklyn Bridge Congestion Trend
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   from_utc_timestamp(window_start, 'America/New_York') AS time_est,
# MAGIC   road_name,
# MAGIC   ROUND(avg_congestion_index, 4) AS congestion_index,
# MAGIC   traffic_status,
# MAGIC   total_observations
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
# MAGIC WHERE road_name = 'Brooklyn Bridge'
# MAGIC ORDER BY window_start DESC

# COMMAND ----------

# DBTITLE 1,Testing Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Testing: End-to-End Alert Verification
# MAGIC Commented-out cells used to test the Slack alerting pipeline end-to-end by inserting a fake GRIDLOCK row into Silver, processing it through Gold, and verifying the Slack alert arrives. Run cleanup after testing. These cells are **not required** for the pipeline job.
# MAGIC
# MAGIC **How to test:**
# MAGIC 1. Uncomment and run Step 1 — inserts a fake row with `currentSpeed=2, freeFlowSpeed=25` → CI = 0.92 (GRIDLOCK)
# MAGIC 2. Run the Gold pipeline cells (Setup → Checkpoint → Schema → Congestion → MERGE)
# MAGIC 3. Check your Slack channel for the alert
# MAGIC 4. Uncomment and run Step 2 — cleans up both Silver and Gold test rows
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **End-to-End Test**: Validates the full data path from Silver insertion through Gold aggregation to Slack notification, confirming that all components (MERGE, CI calculation, recency check, cooldown, webhook) work together.
# MAGIC - **`DELETE FROM`**: A Delta Lake DML operation that removes matching rows. Used here to clean up test data without affecting production records.

# COMMAND ----------

# DBTITLE 1,Step 1: Insert Fake GRIDLOCK into Silver
# # ── TEST: Insert a fake GRIDLOCK row into Silver ──
# # currentSpeed=2, freeFlowSpeed=25 → CI = 1 - (2/25) = 0.92 → GRIDLOCK
# # ingest_time = NOW so the recency check in cell 8 passes (< 30 min old)
# # full_road_name = 'Brooklyn Bridge - TEST' so cleanup is easy

# spark.sql("""
#     INSERT INTO bootcamp_students.zachy_sandeepmanoharan1.silver_traffic
#     (h3_id, currentSpeed, freeFlowSpeed, road_name, osm_id, full_road_name, ingest_time)
#     VALUES
#     ('8a2a1072b597fff', 2, 25, 'Brooklyn Bridge - TEST', 'test_osm', 'Brooklyn Bridge - TEST', current_timestamp())
# """)

# print("Fake GRIDLOCK row inserted into silver_traffic.")
# print("Now run cells 2 → 3 → 6 → 8 to process it through Gold.")
# print("Then check your Slack channel for the alert!")

# COMMAND ----------

# DBTITLE 1,Step 2: Cleanup Test Data
# # ── CLEANUP: Remove the fake GRIDLOCK test data ──
# # Run this AFTER you've verified the Slack alert arrived.

# silver_deleted = spark.sql("""
#     DELETE FROM bootcamp_students.zachy_sandeepmanoharan1.silver_traffic
#     WHERE full_road_name = 'Brooklyn Bridge - TEST'
# """).collect()[0][0]

# gold_deleted = spark.sql("""
#     DELETE FROM bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
#     WHERE road_name = 'Brooklyn Bridge - TEST'
# """).collect()[0][0]

# print(f"Cleaned up: {silver_deleted} row(s) from silver, {gold_deleted} row(s) from gold.")

# COMMAND ----------

# DBTITLE 1,Ad-Hoc Queries Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Ad-Hoc: Recent Congestion Summary
# MAGIC Quick query to check average congestion index by road for the last 2 hours. Useful for spot-checking pipeline output after a job run. This cell is **not required** for the pipeline job.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **`INTERVAL 2 HOURS`**: Databricks SQL syntax for time arithmetic. `current_timestamp() - INTERVAL 2 HOURS` creates a dynamic filter that always looks at the most recent 2-hour window, regardless of when the query runs.

# COMMAND ----------

# DBTITLE 1,Average Congestion Last 2 Hours
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   road_name,
# MAGIC   ROUND(AVG(avg_congestion_index), 4) AS avg_ci,
# MAGIC   COUNT(*) AS windows
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
# MAGIC WHERE window_start > current_timestamp() - INTERVAL 2 HOURS
# MAGIC   AND road_name IS NOT NULL
# MAGIC GROUP BY road_name
# MAGIC ORDER BY avg_ci DESC

# COMMAND ----------

# DBTITLE 1,Optimize Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Maintenance: OPTIMIZE Gold Table
# MAGIC Compacts small Delta files into larger ones for faster dashboard queries. Run this **once** after pausing the job (not on every pipeline run).
# MAGIC
# MAGIC **Why it matters:** Each 10-minute job run appends a small file to the Gold table. After 80+ runs, you have 80+ tiny files. A single `OPTIMIZE` merges them into 1 file, making dashboard queries up to 98% faster by eliminating file-open overhead.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **OPTIMIZE**: A Delta Lake maintenance command that compacts many small files into fewer, larger files. Does NOT change any data — only reorganizes the physical file layout for faster reads.
# MAGIC - **Bin-packing**: The default OPTIMIZE strategy that groups small files into ~1 GB target files. For small tables like `gold_traffic_stats` (~1,400 rows), this typically produces a single file.

# COMMAND ----------

# DBTITLE 1,Optimize Gold Table
# MAGIC %sql
# MAGIC OPTIMIZE bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats;