import json
import time
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError


PROJECT_ID = "tpiuo-lab1" 
SUBSCRIPTION_ID = "reddit.subscription-0036565755"  

# Timeout in seconds for the streaming pull (can be adjusted)
TIMEOUT = 600.0  # 10 minutes

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Callback function executed when a new message is received.
    
    It decodes the message data (which is a bytestring) into a string,
    parses the JSON, and prints the content.
    
    :param message: The Pub/Sub message object.
    """
    try:
        data_string = message.data.decode("utf-8")
        data_json = json.loads(data_string)
        
        print("-" * 50)
        print(f"[{time.strftime('%H:%M:%S')}] Received Message ID: {message.message_id}")

        print(f"JSON Data:\n{json.dumps(data_json, indent=2)}")
        
        if message.attributes:
            print(f"Attributes: {message.attributes}")

        message.ack()
        print("Message Acknowledged.")
        
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not decode message data as JSON. Error: {e}")
        print(f"Raw Data: {message.data}")
        message.nack()
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        message.nack()


def run_subscriber() -> None:
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Listening for messages on {subscription_path}...")
    print(f"The subscriber will run for {TIMEOUT} seconds. Press Ctrl+C to stop.")

    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=callback
    )

    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            print("Subscription timed out after 10 minutes.")
            streaming_pull_future.cancel()  
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            print("Subscriber stopped manually.")
        except Exception as e:
            streaming_pull_future.cancel()
            print(f"An exception occurred during subscription: {e}")

if __name__ == "__main__":
    run_subscriber()