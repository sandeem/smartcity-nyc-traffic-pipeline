# Databricks notebook source
# MAGIC %md
# MAGIC # Part A — Pull Real-Time Traffic Data from TomTom API
# MAGIC
# MAGIC **Pipeline Stage**: Raw Data Ingestion (External API → S3 Landing Zone)
# MAGIC
# MAGIC **What this notebook does**: Calls the TomTom Flow Segment API to fetch live traffic speed data for a GPS point near City Hall / Brooklyn Bridge (NYC), then writes each JSON response to an S3 landing zone with a unique timestamped filename.
# MAGIC
# MAGIC | Detail | Value |
# MAGIC |---|---|
# MAGIC | **API** | TomTom Traffic Flow Segment Data v4 |
# MAGIC | **GPS Point** | 40.7128, -74.0060 (City Hall, NYC) |
# MAGIC | **Output** | One JSON file per pull → S3 landing zone (path from `smartcity-secrets/s3-path`) |
# MAGIC | **Secrets** | `aws-creds` (boto3 S3 client), `tomtom-keys` (API key), `smartcity-secrets` (S3 path) |
# MAGIC | **Schedule** | Triggered by `SmartCity-RT_Pipeline` job every 10 minutes |

# COMMAND ----------

# DBTITLE 1,API Pull Logic
# MAGIC %md
# MAGIC ---
# MAGIC ### TomTom API Pull Loop
# MAGIC
# MAGIC **Logic**: Sends a GET request to TomTom's Flow Segment API, extracts the `flowSegmentData` object (containing `currentSpeed`, `freeFlowSpeed`, and road coordinates), stamps it with `pulled_at`, and writes it as a JSON file to the S3 landing zone.
# MAGIC
# MAGIC **Key details**:
# MAGIC - AWS S3 credentials are stored in Databricks Secrets (`aws-creds` scope) and passed directly to a `boto3.client('s3')` instance for serverless-compatible S3 writes
# MAGIC - API key is stored securely in Databricks Secrets (`tomtom-keys` scope)
# MAGIC - S3 landing path is stored in Databricks Secrets (`smartcity-secrets` scope), parsed into bucket and prefix at runtime
# MAGIC - Each file gets a unique name using Unix timestamp to prevent overwrites
# MAGIC - `iterations` and `delay` control how many pulls and the pause between them
# MAGIC - In job mode, runs once per execution (`iterations=1, delay=0`) since the job itself handles the 10-minute schedule
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **TomTom Flow Segment API**: A REST API that returns real-time traffic speed data for a road segment nearest to a given GPS coordinate. Returns `currentSpeed` (actual), `freeFlowSpeed` (expected with no traffic), and road geometry.
# MAGIC - **S3 Landing Zone**: A raw, unprocessed staging area in Amazon S3 where incoming data lands before being picked up by downstream processing (Auto Loader in Part B).
# MAGIC - **Databricks Secrets** (`dbutils.secrets.get`): A secure key-value store for sensitive credentials. The API key, S3 path, and AWS credentials are never hardcoded in the notebook — they're retrieved at runtime from encrypted scopes.
# MAGIC - **`boto3.client('s3')`**: The AWS SDK for Python. Creates an S3 client using credentials from Databricks Secrets, enabling direct S3 writes (`put_object`) that work on both classic and serverless compute (unlike `spark.conf.set` which is blocked on serverless).
# MAGIC - **`s3_client.put_object`**: A boto3 method that writes an object (file) directly to an S3 bucket. Takes `Bucket`, `Key` (path), `Body` (content), and optional `ContentType`. Replaces `dbutils.fs.put` for serverless compatibility.
# MAGIC - **`time.ctime()`**: Python function that returns the current time as a human-readable string (e.g., `"Wed Mar 26 21:00:09 2026"`). Used as the `pulled_at` timestamp in each JSON file.

# COMMAND ----------

# DBTITLE 1,TomTom API Pull Loop
import requests
import json
import time
import boto3

# AWS S3 Credentials (from Databricks Secrets)
access_key = dbutils.secrets.get(scope="aws-creds", key="access-key")
secret_key = dbutils.secrets.get(scope="aws-creds", key="secret-key")
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

# Configuration
API_KEY = dbutils.secrets.get(scope="tomtom-keys", key="api-key")
POINT = "40.7128,-74.0060"
S3_LANDING_PATH = dbutils.secrets.get(scope="smartcity-secrets", key="s3-path")

# Parse the S3 path into bucket and prefix (e.g., "s3a://bucket/path/" -> bucket="bucket", prefix="path/")
S3_BUCKET = S3_LANDING_PATH.replace("s3a://", "").split("/", 1)[0]
S3_PREFIX = S3_LANDING_PATH.replace("s3a://", "").split("/", 1)[1]

# Define a function to repeat the API data collection (iterations = how many times, delay = seconds between)
def tomtom_pull_loop(iterations=5, delay=60):
    for i in range(iterations):
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={POINT}&key={API_KEY}"
        
        # Send a GET request to TomTom servers to fetch the real-time traffic data
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Convert the raw response text into a Python Dictionary (JSON format)
            data = response.json()
            
            # Extract only the 'flowSegmentData' object, which contains the speed and road info
            # If the key (flowSegmentData) is missing, it returns an empty dictionary {} to prevent the code from crashing
            traffic_info = data.get('flowSegmentData', {})
            
            # Add a new field 'pulled_at' with a human-readable timestamp to track when the data was collected
            traffic_info['pulled_at'] = str(time.ctime())
            
            # Create a unique file key for S3 using the current Unix time to ensure files don't overwrite each other
            file_key = f"{S3_PREFIX}traffic_{int(time.time())}.json"
            
            # Write the JSON string directly to S3 using the boto3 client
            s3_client.put_object(Bucket=S3_BUCKET, Key=file_key, Body=json.dumps(traffic_info), ContentType='application/json')
            
            # Print a success message to the console so you can monitor progress in the notebook
            print(f"Pull {i+1}: Saved to s3a://{S3_BUCKET}/{file_key}")
        else:
            # If the API call fails (e.g., wrong key or server down), print the error code (403, 404, 500, etc.)
            print(f"Error on pull {i+1}: {response.status_code}")
            
        # Check if this is the last iteration; if not, pause the script for the 'delay' duration
        if i < iterations - 1:
            # Pause execution (e.g., for 60 seconds) before making the next API request
            time.sleep(delay)

# JOB MODE: Single pull with no delay (the job schedule handles the 10-minute interval)
tomtom_pull_loop(iterations=1, delay=0)