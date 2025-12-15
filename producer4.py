import os
import requests
import json
import time
from google.cloud import pubsub_v1
import requests.auth

# --- KONFIGURACIJA ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
TOPIC_ID = os.environ.get('PUBSUB_TOPIC_ID')
# 1. NEW: Environment variable for the Dead Letter Topic
DLT_TOPIC_ID = os.environ.get('PUBSUB_DLT_TOPIC_ID') 

API_KEY_JSON_STRING = os.environ.get('API_KEY') 

# URL-ovi
REDDIT_AUTH_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_URL = "https://oauth.reddit.com/r/dataengineering/top/.json?t=all&limit=10"
USER_AGENT = 'DataEngineeringLabScript by /u/YourRedditUsername'

batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1,  # default 100
    max_bytes=1024,  # default 1 MB
    max_latency=1,  # default 10 ms
)

def get_reddit_credentials():
    # ... (No changes to this function) ...
    if not API_KEY_JSON_STRING:
        print("Greška: Nedostaje 'API_KEY' environment varijabla (JSON tajna).")
        raise ValueError("Potrebna je API_KEY tajna za Reddit autentikaciju.")

    try:
        credentials = json.loads(API_KEY_JSON_STRING)
        client_id = credentials.get('client_id') 
        client_secret = credentials.get('client_secret')

        if not client_id or not client_secret:
            print("Greška: JSON tajna ne sadrži 'client_id' ili 'client_secret' polja.")
            raise KeyError("JSON u API_KEY tajni ne sadrži očekivane ključeve.")

        return client_id, client_secret
        
    except json.JSONDecodeError as e:
        print(f"Greška pri parsiranju API_KEY tajne kao JSON: {e}")
        raise
    except KeyError as e:
        print(f"Greška pri čitanju ključeva iz JSON-a: {e}")
        raise

def get_access_token():
    # ... (No changes to this function) ...
    client_id, client_secret = get_reddit_credentials()
    print(" Dohvaćanje Reddit Access Tokena...")
    client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    post_data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": USER_AGENT, }
    try:
        response = requests.post(REDDIT_AUTH_URL,auth=client_auth, data=post_data, headers=headers)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Greška pri dohvaćanju tokena: {e}")
        raise

def fetch_and_publish():
    try:
        access_token = get_access_token()
    except Exception as e:
        print(f"Prekid izvršavanja: {e}")
        return
    
    api_headers = {
        "Authorization": f"bearer {access_token}",
        "User-Agent": USER_AGENT 
    }

    print(f" Povezivanje s Pub/Sub temom: {TOPIC_ID} (Projekt: {PROJECT_ID})")
    publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    # 2. NEW: Setup DLT topic path
    dlt_topic_path = publisher.topic_path(PROJECT_ID, DLT_TOPIC_ID)

    try:
        print(f"Dohvaćanje 10 top objava s OAUTH endpointa...")
        response = requests.get(REDDIT_API_URL, headers=api_headers, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        posts = data['data']['children']
        
        custom_poison_message = {
            "data": {
                "title": "Manual Test for Dead Letter Topic",
                "subreddit": "testing_dlt",
                "author": "tester",
                # ERROR TRIGGER: Schema likely expects 'score' to be int, we send a string.
                "score": "THIS_IS_A_STRING_BUT_SHOULD_BE_INT", 
                "num_comments": 0,
                "id": "test_1",
                "link_flair_text": None,
                "subreddit_name_prefixed": "r/testing"
            }
        }
        
        # Append this custom message to the list of posts to be processed
        posts.append(custom_poison_message)
        # ----------------------------------------

        # 3. CHANGE: List will now hold tuples: (future_object, original_data)
        futures_with_data = [] 
        published_count = 0
        
        FIELDS_TO_KEEP = ["subreddit", "subreddit_name_prefixed",
                          "title", "author", "id", "link_flair_text",
                          "score", "num_comments"]
        
        for post in posts:
            post_data = post.get("data", {})
            try:
                minimal_post = {field: post_data.get(field) for field in FIELDS_TO_KEEP}
            except Exception as e:
                print(f"Error extracting fields from post: {e}")
                continue 
            
            message_data = json.dumps(minimal_post).encode("utf-8")
            print(message_data)
            # Publish to Main Topic
            future = publisher.publish(topic_path, data=message_data)
            future.add_done_callback(
                lambda f: None # Simplified: real status is checked in the loop below
            )
            
            # 4. CHANGE: Store the future AND the data, so we can recover data if it fails
            futures_with_data.append((future, message_data)) 

        # 5. CHANGE: Iterate over the tuple to handle results
        for future, data_payload in futures_with_data:
            try:
                msg_id = future.result() 
                print(f"Poruka ID: {msg_id} uspješno poslana.")
                published_count += 1
            except Exception as e:
                # 6. NEW: Logic to handle Schema Validation errors (and others)
                print(f" Schema/Publish Error (sending to DLT): {e}")
                try:
                    # Send the failed payload to the Dead Letter Topic
                    dlt_future = publisher.publish(dlt_topic_path, data=data_payload)
                    dlt_id = dlt_future.result()
                    print(f" -> Prebačeno na DLT (ID: {dlt_id})")
                except Exception as dlt_e:
                    print(f"CRITICAL: Failed to send to DLT: {dlt_e}")

        publisher.api.transport.close()
        print(f"Uspješno poslano {published_count} objava na main topic.")

    except requests.exceptions.RequestException as e:
        print(f"Greška pri dohvaćanju s Reddita: {e}")
    except Exception as e:
        print(f"Opća greška: {e}")

if __name__ == "__main__":
    fetch_and_publish()
    time.sleep(5)