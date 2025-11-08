import os
import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.api_core import exceptions

# --- Configuration ---
# Get project ID and subscription name from environment variables
# Cloud Run will automatically pass these if set during deployment
PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")

if not PROJECT_ID or not SUBSCRIPTION_ID:
    raise ValueError(
        "Please set the PROJECT_ID and SUBSCRIPTION_ID environment variables."
    )

# The subscription path is a fully qualified resource name
SUBSCRIPTION_PATH = f'projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}'

# Number of seconds the subscriber should listen for messages
# A Cloud Run service should run indefinitely, but a timeout helps for graceful shutdown
TIMEOUT = 300.0  # 5 minutes, will be re-run indefinitely by the loop below

# --- Message Handling Logic ---

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Handles an incoming Pub/Sub message.
    
    Args:
        message: The Pub/Sub message object.
    """
    print(f"Received message ID: {message.message_id}")
    
    try:
        # Decode the message data (Pub/Sub message data is bytes)
        data = message.data.decode('utf-8')
        print(f"  Data: {data}")
        
        # Access message attributes
        if message.attributes:
            print(f"  Attributes: {dict(message.attributes)}")

        # --- Your business logic goes here ---
        # Example: Process the message, write to a database, call an API, etc.
        # time.sleep(1) # Simulate some work

        # Acknowledge the message to tell Pub/Sub it's been processed
        message.ack()
        print(f"  Message {message.message_id} acknowledged.")
        
    except Exception as e:
        print(f"Error processing message {message.message_id}: {e}")
        # Nack the message if processing failed, so Pub/Sub can redeliver it later
        message.nack()
        # You should also implement a dead-letter-topic strategy for persistent failures
        

def run_subscriber() -> None:
    """
    Sets up the streaming pull consumer and keeps it running.
    """
    print(f"Starting Pub/Sub pull consumer for: {SUBSCRIPTION_PATH}")
    
    # Initialize the SubscriberClient
    # This client manages the StreamingPull connection internally
    subscriber = pubsub_v1.SubscriberClient()
    
    try:
        # The 'subscribe' method returns a 'Future' object which manages the streaming connection
        streaming_pull_future = subscriber.subscribe(
            SUBSCRIPTION_PATH, 
            callback=callback
        )
        
        # Keep the main thread alive, but allow the Pub/Sub sub-threads to run
        # In a Cloud Run context, this keeps the container running indefinitely
        while True:
            try:
                # Blocks the main thread until the streaming pull future completes (which shouldn't happen)
                # or a timeout occurs, allowing for a graceful loop
                streaming_pull_future.result(timeout=TIMEOUT)
            except TimeoutError:
                # Expected behavior: The connection is healthy, just loop to keep it alive
                print(f"Still listening after {TIMEOUT} seconds...")
                pass
            except exceptions.NotFound:
                print(f"Subscription {SUBSCRIPTION_PATH} not found. Exiting.")
                break
            
    except Exception as e:
        print(f"An error occurred in the subscriber: {e}")
    finally:
        # Stop the streaming connection
        print("Stopping streaming pull...")
        if 'streaming_pull_future' in locals() and streaming_pull_future:
            streaming_pull_future.cancel()  # Cancels the subscription
        
        # Close the client to free up resources
        if 'subscriber' in locals() and subscriber:
            subscriber.close()

if __name__ == "__main__":
    run_subscriber()