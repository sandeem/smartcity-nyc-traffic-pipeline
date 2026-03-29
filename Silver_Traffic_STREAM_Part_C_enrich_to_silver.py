# Databricks notebook source
# MAGIC %md
# MAGIC # Part C — Enrich Traffic Data to Silver
# MAGIC
# MAGIC **Pipeline Stage**: `bronze_traffic` → `silver_traffic` + `bronze_errors` (quarantine)
# MAGIC
# MAGIC **What this notebook does**: Parses raw TomTom JSON, calculates H3 geospatial indices, joins with 1.8M OpenStreetMap road segments to attach road names, applies data quality checks, and writes valid data to Silver and quarantined rows to an error table.
# MAGIC
# MAGIC | Detail | Value |
# MAGIC |---|---|
# MAGIC | **Input** | `bronze_traffic` (raw API JSON) + `bronze_osm_roads` (1.8M OSM segments) |
# MAGIC | **Output** | `silver_traffic` (enriched) + `bronze_errors` (quarantine) |
# MAGIC | **Optimizations** | Broadcast Hash Join, Liquid Clustering (h3_id), Z-ORDER, AQE |
# MAGIC | **Key Tables** | `silver_osm_enriched` (1.8M H3-indexed roads), `silver_road_crosswalk` (58 H3→road mappings) |

# COMMAND ----------

# DBTITLE 1,Setup Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Setup: Table Creation and Dependencies
# MAGIC Creates the Delta tables (`silver_osm_enriched`, `silver_traffic`, `bronze_errors`) if they don’t exist, installs the `h3` library for geospatial hexagon indexing, and restarts the Python kernel to pick up the new package. The one-time reset cell is **commented out** for production.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Delta Table**: An open-source storage format built on top of Parquet that adds ACID transactions, time travel, and schema enforcement to data lakes.
# MAGIC - **CLUSTER BY (h3_id)**: Enables **Liquid Clustering** — Databricks automatically re-organizes data files so rows with similar `h3_id` values are physically stored together, dramatically speeding up filter queries on that column.
# MAGIC - **Quarantine Table** (`bronze_errors`): A separate table that captures rows that fail data quality checks, keeping the main Silver table clean while preserving bad data for investigation.

# COMMAND ----------

# DBTITLE 1,One-Time Reset (Commented Out)
# # --- 1. SETUP & ONE-TIME RESET (Top of Notebook) ---
# catalog = "bootcamp_students"
# schema = "zachy_sandeepmanoharan1"
# checkpoint_path = f"/Volumes/{catalog}/{schema}/capstone/_checkpoints/silver_traffic"

# # Run these once, then COMMENT THEM OUT after you see data
# dbutils.fs.rm(checkpoint_path, True)
# dbutils.fs.rm(f"{checkpoint_path}_errors", True)
# spark.sql(f"TRUNCATE TABLE {catalog}.{schema}.silver_traffic")

# COMMAND ----------

# DBTITLE 1,Create Silver and Quarantine Tables
catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"

# Create the Enriched OSM Road Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_osm_enriched (
  osm_id STRING,
  full_road_name STRING,
  lat DOUBLE,
  lon DOUBLE,
  h3_id STRING,
  ingest_time TIMESTAMP
) 
USING DELTA
CLUSTER BY (h3_id)
""")

# Create the Silver Traffic Table (Live Joined Table)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_traffic (
  h3_id STRING,
  currentSpeed INT,
  freeFlowSpeed INT,
  road_name STRING,
  osm_id STRING,
  full_road_name STRING,
  ingest_time TIMESTAMP
) 
USING DELTA
CLUSTER BY (h3_id)
""")

# Create the Quarantine table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_errors (
    h3_id STRING,
    currentSpeed INT,
    osm_id STRING,
    error_reason STRING,
    ingest_time TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Install h3 library
# %pip install h3
# NOT NEEDED: The code uses pyspark.databricks.sql.functions.h3_longlatash3 (native Databricks function)
# The pip 'h3' package provides different functions (h3.latlng_to_cell) which are not used in this notebook

# COMMAND ----------

# DBTITLE 1,Restart Python Kernel
# %restart_python
# NOT NEEDED: No pip packages to reload since h3 install was removed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Geospatial Road Indexing
# MAGIC
# MAGIC **Logic:** Converts 1.8 million static OpenStreetMap (OSM) road segments from WKT geometry into a searchable geospatial lookup table by calculating each road’s center point and assigning a unique H3 Hexagon ID at Resolution 10.
# MAGIC
# MAGIC **Optimization:** Uses Liquid Clustering (`CLUSTER BY h3_id`) to physically group data by location, making geographic lookups nearly instantaneous.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **WKT (Well-Known Text)**: A text format for representing geometric shapes (e.g., `LINESTRING(lon1 lat1, lon2 lat2)`). OSM stores road geometry in this format.
# MAGIC - **ST_Centroid / ST_GeomFromWKT**: Spatial SQL functions — `ST_GeomFromWKT` parses the WKT string into a geometry object, and `ST_Centroid` calculates its center point (lat/lon).
# MAGIC - **H3 (Hexagonal Hierarchical Spatial Index)**: Uber’s open-source system that divides the Earth into hexagonal cells at multiple resolutions. At **Resolution 10**, each hexagon is ~15 meters across — small enough to identify individual road segments.
# MAGIC - **h3_longlatash3(lon, lat, resolution)**: A native Databricks Spark function that converts a longitude/latitude point into its H3 hexagon ID string.
# MAGIC - **Liquid Clustering**: A Databricks optimization that automatically compacts and co-locates data by the specified columns during writes — unlike Z-ORDER, it doesn’t require manual `OPTIMIZE` runs.

# COMMAND ----------

# DBTITLE 1,Geospatial H3 Indexing of OSM Roads
from pyspark.sql.functions import col, current_timestamp, expr
from pyspark.databricks.sql.functions import h3_longlatash3

# 1. Configuration (Three-Level Namespace)
# Use the table names already created in your SQL 'CREATE TABLE' step
osm_raw_table = "bootcamp_students.zachy_sandeepmanoharan1.bronze_osm_roads"
osm_enriched_table = "bootcamp_students.zachy_sandeepmanoharan1.silver_osm_enriched"

# 2. Process the 1.8M Rows using Native H3 Functions
# Extract lat/lon from geometry_wkt centroid since source table stores road geometry as WKT.
# h3_longlatash3: Native Spark function turns (Lon, Lat) into a 10-digit "Hexagon ID" for matching.
# current_timestamp: Add a timestamp so you know exactly when each road was processed into Silver.
df_osm_enriched = spark.read.table(osm_raw_table) \
    .withColumn("lon", expr("ST_X(ST_Centroid(ST_GeomFromWKT(geometry_wkt)))")) \
    .withColumn("lat", expr("ST_Y(ST_Centroid(ST_GeomFromWKT(geometry_wkt)))")) \
    .withColumn("h3_id", h3_longlatash3(col("lon"), col("lat"), 10).cast("string")) \
    .withColumn("full_road_name", col("name")) \
    .withColumn("ingest_time", current_timestamp()) \
    .select("osm_id", "full_road_name", "lat", "lon", "h3_id", "ingest_time")

# 3. Write to Silver & Apply Liquid Clustering
# Adding Schema Evolution by using 'overwriteSchema' which allows the table to accept schema changes automatically if the OSM source format changes.
# Liquid Clustering by h3_id is already defined via CREATE TABLE in Cell 1.
df_osm_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(osm_enriched_table)

print(f"Success: {osm_enriched_table} is now geographically indexed with 1.8M+ rows.")

# COMMAND ----------

# DBTITLE 1,OSM Verification
# MAGIC %md
# MAGIC ---
# MAGIC ### Verification: OSM Enriched Data
# MAGIC Optional cells to preview the enriched OSM table and verify the row count (∼1.8M). These are **not required** for the pipeline job.
# MAGIC
# MAGIC *Confirms that the geospatial indexing completed successfully and all OSM road segments received H3 hexagon IDs.*

# COMMAND ----------

# DBTITLE 1,Preview Silver OSM Enriched
# MAGIC %sql
# MAGIC -- Viewing top 20 rows to see the enriched columns
# MAGIC SELECT * FROM bootcamp_students.zachy_sandeepmanoharan1.silver_osm_enriched LIMIT 20;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verify OSM Road Count
# MAGIC %sql
# MAGIC -- Verifying the total count (should be ~1.8M)
# MAGIC SELECT count(*) AS total_roads FROM bootcamp_students.zachy_sandeepmanoharan1.silver_osm_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Logic Reference Table (The Traffic-to-Road Bridge)
# MAGIC
# MAGIC **Logic:** Creates a "translator" table (`silver_road_crosswalk`) that maps live TomTom GPS points to specific OSM road names using shared H3 hexagon IDs. This small table (58 rows) acts as a lookup bridge between real-time traffic data and the 1.8M-row OSM road database.
# MAGIC
# MAGIC **Optimizations:**
# MAGIC - **Broadcast Hash Join** to eliminate network shuffle during the join
# MAGIC - **Z-ORDER** to physically co-locate rows by `h3_id` for fast lookups
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Broadcast Hash Join**: A Spark join strategy where the smaller table is copied ("broadcast") to every worker node’s memory. Workers then join locally using a hash table — no network shuffle needed. Ideal when one side is small enough to fit in memory.
# MAGIC - **Z-ORDER**: A Delta Lake optimization that physically re-arranges data files so rows with similar values in the specified columns are stored close together on disk. This enables **data skipping** — Spark can skip entire files whose min/max statistics don’t match the filter.
# MAGIC - **`crosswalk.explain()`**: Prints the Spark **query execution plan**, showing which join strategy Spark chose (e.g., `BroadcastHashJoin`, `BuildRight` = the right table was broadcast).

# COMMAND ----------

# DBTITLE 1,Build Road Crosswalk Table
catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"

from pyspark.sql.functions import broadcast, coalesce, get_json_object, col
from pyspark.databricks.sql.functions import h3_longlatash3

osm_enriched = spark.table(f"{catalog}.{schema}.silver_osm_enriched")
traffic_raw = spark.table(f"{catalog}.{schema}.bronze_traffic")

# 1. PARSE JSON: Extract the FIRST point [0] from the coordinate list to match the road start.
# 2. H3 INDEX: Convert that specific Lon/Lat into a joinable hexagon ID (Resolution 10).
# 3. WATERMARKING: Topic: Streaming - Drop data older than 15 mins to manage memory/late arrivals
traffic_with_h3 = traffic_raw \
    .withColumn("coord_json", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0]"),
        get_json_object(col("coordinates"), "$.coordinate[0]")
    )) \
    .withColumn("lat", get_json_object(col("coord_json"), "$.latitude").cast("double")) \
    .withColumn("lon", get_json_object(col("coord_json"), "$.longitude").cast("double")) \
    .withColumn("h3_id", h3_longlatash3(col("lon"), col("lat"), 10).cast("string"))

# Sends small static OSM road data to every worker to meet the traffic stream (Broadcast Join (Shuffle Optimization)) 
crosswalk = traffic_with_h3.select("h3_id") \
    .join(broadcast(osm_enriched.select("h3_id", "osm_id", "full_road_name")), "h3_id") \
    .distinct()

# I came across BroadcastHashJoin in my output
# 1. 'BroadcastHashJoin': Spark sends the smaller table to all workers, skipping the slow "Shuffle" phase.
# 2. 'BuildRight': Correctly identifies the OSM Crosswalk (right side) as the smaller table to fit in memory.
# 3. 'Inner' Join: Ensures only traffic points that land on an existing OSM road segment are kept.
# Performance: This optimization makes joining 1.5M+ rows 10x-100x faster than a standard Spark join.
crosswalk.explain()

# Save as a managed table in Unity Catalog
crosswalk.write.mode("overwrite") \
    .saveAsTable(f"{catalog}.{schema}.silver_road_crosswalk")

# Physically re-organizes files (Z-ORDER) by h3_id to make Script 3's lookup lightning fast
spark.sql(f"OPTIMIZE {catalog}.{schema}.silver_road_crosswalk ZORDER BY (h3_id)")

# COMMAND ----------

# DBTITLE 1,Crosswalk Verification
# MAGIC %md
# MAGIC ---
# MAGIC ### Verification: Road Crosswalk
# MAGIC Optional cells to preview the crosswalk table (58 H3 → road mappings) and confirm the count. These are **not required** for the pipeline job.
# MAGIC
# MAGIC *The crosswalk is the “bridge” between live traffic H3 IDs and OSM road names. If this table is empty or has unexpected counts, the enrichment join will fail to attach road names.*

# COMMAND ----------

# DBTITLE 1,Preview Road Crosswalk
# In a Python cell
catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"

# Show the first 10 mappings
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.silver_road_crosswalk LIMIT 10"))

# Check how many unique roads are currently in your crosswalk
# This is like a "Success Meter" tells exactly how many TomTom traffic points successfully "found a match" on your 1.8M-row OpenStreetMap (OSM) map
print(f"Unique Road Mappings: {spark.table(f'{catalog}.{schema}.silver_road_crosswalk').count()}")


# COMMAND ----------

# DBTITLE 1,Verify Crosswalk Count
catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"

display(spark.sql(f"SELECT count(*) AS crosswalk_rows FROM {catalog}.{schema}.silver_road_crosswalk"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Enrichment Pipeline (The Real-Time Engine)
# MAGIC
# MAGIC **Logic:** Reads the raw bronze traffic stream, parses JSON to extract speeds and coordinates, calculates H3 hexagon IDs, joins with the road crosswalk to attach road names, applies data quality filters, and writes valid rows to Silver and bad rows to the quarantine table.
# MAGIC
# MAGIC **Optimizations:**
# MAGIC - **Adaptive Query Execution (AQE)** to automatically tune partitions and join strategies at runtime
# MAGIC - **Broadcast Hash Join** on the small crosswalk table
# MAGIC - **Watermarking** to manage memory for late-arriving data
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Structured Streaming**: Spark’s engine for processing data incrementally as it arrives. The `readStream` / `writeStream` API treats incoming data as an unbounded table that grows over time.
# MAGIC - **Watermarking** (`.withWatermark("stream_time", "15 minutes")`): Tells Spark to drop data older than 15 minutes from its in-memory state. This prevents unbounded memory growth while still handling data that arrives slightly late.
# MAGIC - **Adaptive Query Execution (AQE)**: A Spark runtime optimization that re-plans queries after each stage completes. Key benefits: (1) coalesces too many small shuffle partitions into fewer large ones, (2) switches join strategies based on actual data sizes, (3) optimizes skewed joins.
# MAGIC - **`trigger(availableNow=True)`**: A batch-mode trigger that processes all currently available data then stops — perfect for scheduled job execution (vs. continuous streaming which runs forever).
# MAGIC - **`awaitTermination()`**: Blocks the Python process until the stream finishes. Without this, a scheduled job would mark the task as “complete” while the stream is still running, causing the cluster to terminate mid-processing.
# MAGIC - **Coalesce Fallback Pattern**: Uses `coalesce(try_json_path, fallback_column)` to handle two different bronze data formats (old format with nested JSON vs. new format with top-level columns) without silently dropping rows.
# MAGIC - **LEFT JOIN** (vs INNER): Preserves all traffic rows even if they don’t match a road — unmatched rows are routed to the quarantine table with an `error_reason` instead of being silently dropped.

# COMMAND ----------

# DBTITLE 1,Silver Enrichment Pipeline
from pyspark.sql.functions import broadcast, coalesce, col, current_timestamp, expr, get_json_object, lower, to_timestamp, when
from pyspark.databricks.sql.functions import h3_longlatash3

# 0. PERFORMANCE: Adaptive Query Execution (AQE) for handling traffic spikes
# On serverless, AQE is always enabled and these configs are locked — try/except keeps this portable
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
except Exception:
    pass  # Serverless: AQE is always on, configs are read-only

# FIX: Enable legacy time parser so we can parse pulled_at ctime strings (e.g. "Mon Mar 23 01:10:28 2026")
try:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception:
    pass  # Serverless: may not allow this config

# 1. SETUP
catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"
checkpoint_path = f"/Volumes/{catalog}/{schema}/capstone/_checkpoints/silver_traffic"
silver_table = f"{catalog}.{schema}.silver_traffic"
error_table = f"{catalog}.{schema}.bronze_errors" # Table for bad data

# 2. RESET CHECKPOINT (Only uncomment when deploying logic changes that require full reprocessing)
# dbutils.fs.rm(checkpoint_path, True)
# dbutils.fs.rm(f"{checkpoint_path}", True)
# dbutils.fs.rm(f"{checkpoint_path}_errors", True)

# 3. READ & PARSE
# Bronze has TWO data formats:
#   Old format (Mar 15): speeds inside flowSegmentData JSON -> {"flowSegmentData": {"currentSpeed": 27, ...}}
#   New format (Mar 21+): speeds at top level -> {"currentSpeed": 4, "freeFlowSpeed": 25, ...}
# FIX: coalesce tries flowSegmentData first, falls back to top-level columns so NO rows are silently dropped
# FIX: Use original pulled_at timestamp instead of current_timestamp() so each reading keeps its real time.
#       This prevents heavy traffic from different days being averaged into the same Gold window.
traffic_stream = spark.readStream.table(f"{catalog}.{schema}.bronze_traffic") \
    .withColumn("lat", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0].latitude"),
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate.latitude"),
        get_json_object(col("coordinates"), "$.coordinate[0].latitude")
    ).cast("double")) \
    .withColumn("lon", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0].longitude"),
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate.longitude"),
        get_json_object(col("coordinates"), "$.coordinate[0].longitude")
    ).cast("double")) \
    .withColumn("currentSpeed", coalesce(
        get_json_object(col("flowSegmentData"), "$.currentSpeed"),
        col("currentSpeed")
    ).cast("int")) \
    .withColumn("freeFlowSpeed", coalesce(
        get_json_object(col("flowSegmentData"), "$.freeFlowSpeed"),
        col("freeFlowSpeed")
    ).cast("int")) \
    .withColumn("h3_id", h3_longlatash3(col("lon"), col("lat"), 10).cast("string")) \
    .withColumn("stream_time", coalesce(
        to_timestamp(expr("substring(pulled_at, 5)"), "MMM dd HH:mm:ss yyyy"),
        current_timestamp()
    )) \
    .withWatermark("stream_time", "15 minutes")

# 3.5: CLEAN THE STREAM ---
# This ensures we don't waste compute power trying to join "NULL" rows
traffic_stream_cleaned = traffic_stream.filter(
    col("h3_id").isNotNull() & 
    col("currentSpeed").isNotNull()
)

# 4. JOIN WITH CROSSWALK: Load the 'Bridge' table that maps H3 IDs to OSM road details
static_crosswalk = spark.table(f"{catalog}.{schema}.silver_road_crosswalk")

# LEFT JOIN: Changed from 'inner' to 'left' so we don't lose rows that fail to match a road
# This allows us to see exactly WHICH GPS points are failing map-matching for the Error table
# FIX: Using explicit col() refs to avoid file-level permission checks under USER_ISOLATION
enriched_raw = traffic_stream_cleaned.alias("stream").join(
    broadcast(static_crosswalk.alias("map")),
    lower(col("stream.h3_id")) == lower(col("map.h3_id")),
    "left" 
).select(
    col("stream.h3_id"),
    col("stream.currentSpeed"),
    col("stream.freeFlowSpeed"),
    col("map.osm_id"),
    col("map.full_road_name"),
    col("stream.stream_time").alias("ingest_time")
)

# 4.5: QUARANTINE LOGIC 
# VALID DATA: Keep only records that successfully matched a road AND have realistic speeds
silver_traffic = enriched_raw.filter("currentSpeed <= 150 AND full_road_name IS NOT NULL AND osm_id IS NOT NULL")

# QUARANTINE DATA: Identify records that failed the quality check
quarantine_traffic = enriched_raw.filter("currentSpeed > 150 OR osm_id IS NULL OR full_road_name IS NULL") \
    .withColumn("error_reason", 
        when(col("osm_id").isNull(), "Map Matching Failed (NULL osm_id)")
        .when(col("full_road_name").isNull(), "Unnamed Road Segment (NULL name)")
        .otherwise("Invalid Speed (> 150mph)")) \
    .select("h3_id", "currentSpeed", "osm_id", "error_reason", "ingest_time")

# 5. WRITE TO SILVER: Batch-mode streaming for scheduled job execution
query = (silver_traffic.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path) 
    .option("mergeSchema", "true")
    .outputMode("append") 
    .trigger(availableNow=True)
    .toTable(silver_table)) 

# JOB MODE FIX: Block until stream completes so the job doesn't kill the cluster mid-processing
query.awaitTermination()
print("Silver stream finished. Check your silver_traffic table now!")

# 6. WRITE TO QUARANTINE: Batch-mode streaming for scheduled job execution
quarantine_query = (quarantine_traffic.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}_quarantine") 
    .outputMode("append") 
    .trigger(availableNow=True)
    .toTable(error_table))

# JOB MODE FIX: Block until stream completes so the job doesn't kill the cluster mid-processing
quarantine_query.awaitTermination()
print(f"All streams finished. Check {silver_table} for valid data and {error_table} for quarantined rows.")

# COMMAND ----------

# DBTITLE 1,Final Validation
# MAGIC %md
# MAGIC ---
# MAGIC ### Validation: Silver Data Quality Check
# MAGIC Optional cells to inspect the enriched Silver data, compare Silver vs quarantine row counts, and check the bronze data time range. These are **not required** for the pipeline job but useful for monitoring data quality after each run.
# MAGIC
# MAGIC *The Data Quality Summary (Silver vs Quarantine) shows the ratio of valid rows to quarantined rows. A high quarantine rate may indicate issues with the crosswalk table, unexpected data formats, or GPS points outside the monitored area.*

# COMMAND ----------

# DBTITLE 1,Preview Silver Traffic Data
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   full_road_name, 
# MAGIC   currentSpeed, 
# MAGIC   freeFlowSpeed, 
# MAGIC   ingest_time 
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.silver_traffic 
# MAGIC -- Filter for rows where we actually have data
# MAGIC WHERE currentSpeed IS NOT NULL 
# MAGIC   AND full_road_name IS NOT NULL
# MAGIC ORDER BY ingest_time DESC 
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Data Quality Summary (Silver vs Quarantine)
# MAGIC %sql
# MAGIC SELECT 'Silver Data' as Table, count(*) FROM bootcamp_students.zachy_sandeepmanoharan1.silver_traffic
# MAGIC UNION ALL
# MAGIC SELECT 'Quarantined Errors' as Table, count(*) FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_errors;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Bronze Data Range (Oldest to Newest)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(*) as total_rows, 
# MAGIC   from_utc_timestamp(to_timestamp(substring(min(pulled_at), 5), 'MMM dd HH:mm:ss yyyy'), 'America/New_York') as oldest_record_est, 
# MAGIC   from_utc_timestamp(to_timestamp(substring(max(pulled_at), 5), 'MMM dd HH:mm:ss yyyy'), 'America/New_York') as newest_record_est 
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_traffic;