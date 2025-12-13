import os
from concurrent.futures import TimeoutError
import threading

# --- Flask addition ---
from flask import Flask

from google.cloud import pubsub_v1
from google.api_core import exceptions

# --- Configuration ---
# Get project ID and subscription name from environment variables
PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
# Cloud Run will set the PORT environment variable
PORT = int(os.environ.get("PORT", 8080))

if not PROJECT_ID or not SUBSCRIPTION_ID:
    raise ValueError(
        "Please set the PROJECT_ID and SUBSCRIPTION_ID environment variables."
    )

SUBSCRIPTION_PATH = f'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'
TIMEOUT = 300.0

# Initialize Flask app
app = Flask(__name__)

# --- 1. Cloud Run Health Check Endpoint (Required for Deployment) ---
@app.route("/", methods=["GET"])
def health_check():
    """
    Minimal endpoint to satisfy Cloud Run's liveness and readiness checks.
    """
    return "Pub/Sub Worker is Running", 200

# --- 2. Pub/Sub Message Handling Logic (Your Core Business Logic) ---

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Handles an incoming Pub/Sub message."""
    print(f"Received message ID: {message.message_id}")

    try:
        data = message.data.decode('utf-8')
        print(f"  Data: {data}")

        if message.attributes:
            print(f"  Attributes: {dict(message.attributes)}")

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
