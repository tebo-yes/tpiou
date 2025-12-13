import os
import requests
import json
import time
from google.cloud import pubsub_v1
import requests.auth

# --- KONFIGURACIJA ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
TOPIC_ID = os.environ.get('PUBSUB_TOPIC_ID')
# NOVA TAJNA: Pretpostavljamo da ova EV sadrži cijeli JSON string
API_KEY_JSON_STRING = os.environ.get('API_KEY')

# URL-ovi
REDDIT_AUTH_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_URL = "https://oauth.reddit.com/r/dataengineering/top/.json?t=all&limit=10"
USER_AGENT = 'DataEngineeringLabScript by /u/YourRedditUsername'

def get_reddit_credentials():
    """Parsira JSON string iz environment varijable API_KEY da dobije Client ID i Secret."""

    if not API_KEY_JSON_STRING:
        print("Greška: Nedostaje 'API_KEY' environment varijabla (JSON tajna).")
        raise ValueError("Potrebna je API_KEY tajna za Reddit autentikaciju.")

    try:
        # 1. Parsiranje JSON stringa u Python rječnik
        credentials = json.loads(API_KEY_JSON_STRING)

        # 2. Ekstrakcija ključeva iz rječnika. PRILAGODITE OVE KLJUČEVE
        # ako se u vašoj JSON datoteci/tajni zovu drugačije (npr. 'client_id_key', 'client_secret_key').
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
    """Dohvaća OAuth2 pristupni token koristeći Client Credentials."""

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
    """Glavna funkcija za dohvaćanje i objavljivanje."""

    try:
        access_token = get_access_token()
    except Exception as e:
        print(f"Prekid izvršavanja: {e}")
        return

    # ... (ostatak logike za Pub/Sub i dohvat s Reddita ostaje isti, koristeći access_token)

    # [Logika za Pub/Sub i Reddit API ide ovdje]

    # 2. POSTAVLJANJE ZAGLAVLJA ZA AUTENTIFICIRANI API ZAHTJEV
    api_headers = {
        "Authorization": f"bearer {access_token}",
        "User-Agent": USER_AGENT
    }

    print(f" Povezivanje s Pub/Sub temom: {TOPIC_ID} (Projekt: {PROJECT_ID})")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    try:
        # 3. DOHVAT PODATAKA S REDDITA (KORISTEĆI OAUTH ENDPOINT)
        print(f"Dohvaćanje 10 top objava s OAUTH endpointa...")
        response = requests.get(REDDIT_API_URL, headers=api_headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        posts = data['data']['children']
        futures = []
        published_count = 0

        # 4. SLANJE NA PUB/SUB
        for post in posts:
            message_data = json.dumps(post).encode("utf-8")
            future = publisher.publish(topic_path, data=message_data)
            future.add_done_callback(
                lambda f: print(f"Poruka ID: {f.result()} poslana.")
            )
            futures.append(future)
            published_count += 1

        for future in futures:
            try:
                # Čekanje rezultata (ID poruke) osigurava da je slanje završeno
                future.result()
            except Exception as e:
                print(f" Greška pri slanju poruke: {e}")
        publisher.api.transport.close()
        print(f"Uspješno poslano {published_count} objava na Pub/Sub.")

    except requests.exceptions.RequestException as e:
        print(f"Greška pri dohvaćanju s Reddita: {e}")
    except Exception as e:
        print(f"Opća greška: {e}")

if __name__ == "__main__":
    fetch_and_publish()
    # ... (Beskonačna petlja, ako je potrebna za Zadatak 1)
    time.sleep(5)
