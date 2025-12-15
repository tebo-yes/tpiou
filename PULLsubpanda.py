import os
from concurrent.futures import TimeoutError
import threading
import datetime
# --- Flask addition ---
from flask import Flask
import pandas as pd
from google.cloud import pubsub_v1, storage, bigquery
from google.api_core import exceptions

# --- Configuration ---
# Get project ID and subscription name from environment variables
PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME")
DATASET_NAME = os.environ.get("GCP_DS_NAME")
# Cloud Run will set the PORT environment variable
PORT = int(os.environ.get("PORT", 8080))

if not PROJECT_ID or not SUBSCRIPTION_ID:
    raise ValueError(
        "Please set the PROJECT_ID and SUBSCRIPTION_ID environment variables."
    )

SUBSCRIPTION_PATH = f'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'

# In-memory buffer for messages
TIMEOUT = 300.0 
message_buffer = []
buffer_lock = threading.Lock()
# Initialize Flask app
app = Flask(__name__)

# --- 1. Cloud Run Health Check Endpoint (Required for Deployment) ---
@app.route("/", methods=["GET"])
def health_check():
    """
    Minimal endpoint to satisfy Cloud Run's liveness and readiness checks.
    """
    return "Pub/Sub Worker is Running", 200

def write_to_gcs(data_list) -> str:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    timestamp_for_filename = now_utc.strftime("%Y%m%d%H%M%S")
    
    # Define the partition components
    year = now_utc.strftime("%Y")
    month = now_utc.strftime("%m")
    day = now_utc.strftime("%d")
    hour = now_utc.strftime("%H")
    
    if not data_list:
        return f"gs://{BUCKET_NAME} (No records provided, file not created)"

    try:
        df = pd.DataFrame(data_list)
        df['processing_timestamp'] = pd.to_datetime('now', utc=True)

        parquet_buffer = pd.io.common.BytesIO()
        df.columns = df.columns.astype(str)
        df.to_parquet(parquet_buffer, index=False, compression='snappy')
        parquet_buffer.seek(0) # Rewind the buffer to the beginning

        # Structure: folder_prefix/year=YYYY/month=MM/day=DD/hour=HH/filename.parquet
        partition_prefix = f"year={year}/month={month}/day={day}/hour={hour}"
        file_name = f"{timestamp_for_filename}-{len(data_list)}.parquet"
        
        # Use a top-level prefix like 'pubsub_parquet' or similar
        blob_name = f"pubsub_parquet/{partition_prefix}/{file_name}"

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)

        blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
        gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
        print(f"Successfully uploaded {len(df)} records to Parquet file: {gcs_uri}")

        return gcs_uri
    except Exception as e:
        print(f"An error occurred during data processing or upload: {e}")
        raise e


def send_to_BigQuery(temp_uri) -> None:
    bq_client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.newtable"
    gcs_uri = temp_uri
    job_config = bigquery.LoadJobConfig(
       source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
        )
    try: 
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded Parquet from GCS to BigQuery: {gcs_uri} -> {table_id}")
    except Exception as e:
        print(f"BigQuery Load Job failed for {gcs_uri}: {e}")



# --- 2. Pub/Sub Message Handling Logic (Your Core Business Logic) ---
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Handles an incoming Pub/Sub message."""
    print(f"Received message ID: {message.message_id}")
    
    try:
        data = message.data.decode('utf-8')
        print(f"  Data: {data}")
        if message.attributes:
            print(f"  Attributes: {dict(message.attributes)}")

        with buffer_lock:
            message_buffer.append(data)
            # If you want to write immediately for every message:
            temp_uri = write_to_gcs(list(message_buffer)) # Pass a copy of the list
            message_buffer.clear()
            send_to_BigQuery(temp_uri=temp_uri)
        # Acknowledge the message
        
        message.ack()
        print(f"  Message {message.message_id} acknowledged.")
        
    except Exception as e:
        print(f"Error processing message {message.message_id}: {e}")
        message.nack()
        

def run_subscriber() -> None:
    """
    Sets up the streaming pull consumer and keeps it running.
    This runs in the MAIN thread.
    """
    print(f"Starting Pub/Sub pull consumer for: {SUBSCRIPTION_PATH}")
    
    subscriber = pubsub_v1.SubscriberClient()
    
    try:
        streaming_pull_future = subscriber.subscribe(
            SUBSCRIPTION_PATH, 
            callback=callback
        )
        # Keep the main thread alive using the streaming pull future
        while True:
            try:
                # Keep the connection open
                streaming_pull_future.result(timeout=TIMEOUT)
            except TimeoutError:
                print(f"Still listening after {TIMEOUT} seconds...")
                pass
            except exceptions.NotFound:
                print(f"Subscription {SUBSCRIPTION_PATH} not found. Exiting.")
                break
    except Exception as e:
        print(f"An error occurred in the subscriber: {e}")     
    finally:
        print("Stopping streaming pull...")
        if 'streaming_pull_future' in locals() and streaming_pull_future:
            streaming_pull_future.cancel()  # Cancels the subscription
        
        # Close the client to free up resources
        if 'subscriber' in locals() and subscriber:
            subscriber.close()

# --- 3. Execution Block ---

def start_flask_server():
    """
    Runs the Flask app on a separate thread to satisfy the Cloud Run health check.
    """
    print(f"Starting Flask health server on 0.0.0.0:{PORT}")
    # Use 'app.run' with host and port from environment. 'debug=False' is important.
    app.run(host='0.0.0.0', port=PORT, debug=False)

if __name__ == "__main__":
    # Start the Flask server in a background thread
    # The 'daemon=True' ensures the thread is killed when the main thread exits
    flask_thread = threading.Thread(target=start_flask_server, daemon=True)
    flask_thread.start()
    
    # Start the continuous Pub/Sub worker in the main thread
    run_subscriber()