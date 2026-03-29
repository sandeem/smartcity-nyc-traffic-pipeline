# Databricks notebook source
# MAGIC %md
# MAGIC # Part B — Stream Raw JSON Files to Bronze Delta Table
# MAGIC
# MAGIC **Pipeline Stage**: S3 Landing Zone → `bronze_traffic` (Delta)
# MAGIC
# MAGIC **What this notebook does**: Uses Databricks Auto Loader (`cloudFiles`) to incrementally detect new JSON files in S3, infer their schema, and append them to a managed Unity Catalog Delta table.
# MAGIC
# MAGIC | Detail | Value |
# MAGIC |---|---|
# MAGIC | **Source** | S3 landing zone (path from `smartcity-secrets/s3-path`) |
# MAGIC | **Target** | `bootcamp_students.zachy_sandeepmanoharan1.bronze_traffic` |
# MAGIC | **Secrets** | `aws-creds` (reader-scoped S3 options), `smartcity-secrets` (S3 path) |
# MAGIC | **Method** | Auto Loader with `trigger(availableNow=True)` for job-safe batch streaming |
# MAGIC | **Schema** | Auto-inferred and stored at checkpoint path |

# COMMAND ----------

# DBTITLE 1,Maintenance Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Maintenance: Checkpoint Reset
# MAGIC Reset the Auto Loader checkpoint to reprocess all files from scratch. **Only uncomment when needed** (e.g., schema changes or corrupted checkpoint). In production, this stays commented out to preserve exactly-once processing guarantees.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Checkpoint**: A directory where Spark stores the processing state of a streaming query — which files have been seen, which offsets have been committed. On restart, Spark reads this directory to resume where it left off, guaranteeing **exactly-once processing** (no duplicates, no missed files).
# MAGIC - **`dbutils.fs.rm(path, True)`**: Recursively deletes a directory. Deleting the checkpoint forces the stream to reprocess all source files from the beginning.

# COMMAND ----------

# DBTITLE 1,Bronze Checkpoint Reset (One-Time)
# dbutils.fs.rm("/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/_checkpoints/bronze_traffic", True)

# COMMAND ----------

# DBTITLE 1,Core Pipeline Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Core Pipeline: Auto Loader → Bronze Table
# MAGIC
# MAGIC **Logic**: Auto Loader (`cloudFiles` format) watches the S3 landing zone for new JSON files, infers schema on first run (stored at checkpoint path), and appends new records to the managed Delta table.
# MAGIC
# MAGIC **Key fixes applied**:
# MAGIC - Changed `.start(path)` → `.toTable(bronze_table)` to write directly to the UC-managed table
# MAGIC - Added `trigger(availableNow=True)` + `awaitTermination()` for job-safe batch streaming
# MAGIC - AWS S3 credentials retrieved from Databricks Secrets (`aws-creds` scope) and passed as **reader-scoped `.option()` calls** on the Auto Loader stream — serverless-compatible (unlike `spark.conf.set` which is blocked)
# MAGIC - S3 landing path retrieved from Databricks Secrets (`smartcity-secrets` scope) — no hardcoded paths
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Auto Loader** (`cloudFiles`): A Databricks feature that incrementally ingests new files from cloud storage. Uses file notification (S3 events) or directory listing to detect new files — far more efficient than listing the entire directory on every run.
# MAGIC - **Schema Inference** (`schemaLocation`): On the first run, Auto Loader reads a sample of files to infer the JSON schema and stores it at the checkpoint path. On subsequent runs, it reuses this schema, only evolving it if new columns appear (`mergeSchema`).
# MAGIC - **Reader-Scoped S3 Options** (`.option("fs.s3a.*")`): Passes AWS credentials directly to the Auto Loader reader instead of setting them globally via `spark.conf.set`. This is required on serverless compute, where global Hadoop configs are blocked.
# MAGIC - **Unity Catalog Managed Table** (`.toTable()`): A table whose data files and metadata are fully managed by Unity Catalog. Unlike `.start(path)` which writes to an external location, `.toTable()` ensures the catalog’s metadata stays in sync with the actual data.
# MAGIC - **`trigger(availableNow=True)`**: Tells the streaming query to process all currently available files, then stop. This is the job-safe alternative to continuous streaming — the query terminates cleanly after processing, allowing the job to move to the next task.
# MAGIC - **`awaitTermination()`**: Blocks the Python thread until the stream finishes. Without this, the job scheduler marks the task as "done" immediately, potentially killing the cluster while the stream is still writing.

# COMMAND ----------

# DBTITLE 1,Auto Loader S3 to Bronze
from pyspark.sql.functions import *

# AWS S3 Credentials (from Databricks Secrets)
access_key = dbutils.secrets.get(scope="aws-creds", key="access-key")
secret_key = dbutils.secrets.get(scope="aws-creds", key="secret-key")

# 1. Source (Where the raw JSONs are)
source_path = dbutils.secrets.get(scope="smartcity-secrets", key="s3-path")

# 2. Target: Managed Unity Catalog table
bronze_table = "bootcamp_students.zachy_sandeepmanoharan1.bronze_traffic"

# 3. Checkpoint (Must be separate to track progress)
checkpoint_path = "/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/_checkpoints/bronze_traffic"

# Auto Loader Configuration
# FIX: Pass S3 credentials as reader-scoped options instead of spark.conf.set (blocked on serverless)
df_stream = (spark.readStream
  .format("cloudFiles")           # Use "Auto Loader," a smart Databricks tool.
  .option("cloudFiles.format", "json") # Expect JSON files.
  .option("cloudFiles.schemaLocation", checkpoint_path) # Remember the file structure.
  .option("fs.s3a.access.key", access_key)
  .option("fs.s3a.secret.key", secret_key)
  .option("fs.s3a.endpoint", "s3.amazonaws.com")
  .load(source_path))             # Point it at your S3 "Landing Zone."

# FIX: Write to the managed Unity Catalog table using .toTable() instead of .start(volume_path).
# .start(path) writes to a file location, creating a disconnect from the managed table.
# .toTable() writes directly to the UC table, keeping the data pipeline consistent.
query = (df_stream.writeStream
  .format("delta")                # Save the data in "Delta" format.
  .option("checkpointLocation", checkpoint_path) 
  .trigger(availableNow=True)     # Tells Spark to process new items, then shut down.
  .outputMode("append")           
  .toTable(bronze_table))         # Write directly to the managed UC table.

# JOB MODE FIX: Block until stream completes so the job doesn't kill the cluster mid-processing
query.awaitTermination()
print(f"Bronze stream finished. {bronze_table} is up to date.")

# COMMAND ----------

# DBTITLE 1,Verification Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Verification: Query Bronze Data
# MAGIC Optional cells to inspect the raw bronze data after ingestion and verify the row count. These cells are **not required** for the pipeline job.
# MAGIC
# MAGIC Since the pipeline writes directly to the managed Unity Catalog table via `.toTable()`, we query the table directly — no SQL view is needed.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Bronze Layer**: The first layer of the Medallion Architecture. Contains raw, unprocessed data exactly as it arrived from the source — no transformations, no filtering. Serves as the "system of record" for replay and debugging.

# COMMAND ----------

# DBTITLE 1,Query Bronze Table
# MAGIC %sql
# MAGIC -- Query the managed Unity Catalog table directly
# MAGIC -- (No view needed — the pipeline writes via .toTable() to the UC-managed table)
# MAGIC SELECT * FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_traffic
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Verify Bronze Row Count
# MAGIC %sql
# MAGIC -- Verify total row count in the Bronze table
# MAGIC SELECT count(*) AS total_bronze_rows FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_traffic;