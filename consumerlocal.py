import os
import json
from google.cloud import pubsub_v1

# --- KONFIGURACIJA (KORISTI ENVIRONMENT VARIJABLE) ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
SUBSCRIPTION_ID = os.environ.get('PUBSUB_SUBSCRIPTION_ID')
# Ograničenje za lokalno povlačenje (pull)
TIMEOUT = 5.0 # Sekundi

def callback(message):
    """Obrađuje primljenu Pub/Sub poruku."""
    try:
        # Dekodiranje i parsiranje poruke
        data_str = message.data.decode('utf-8')
        post = json.loads(data_str)
        
        # Ispisivanje podataka
        print("-" * 50)
        print(f"Nova objava primljena (Poruka ID: {message.message_id})")
        print(f"Naslov: {post['data'].get('title', 'N/A')}")
        print(f"Subreddit: r/{post['data'].get('subreddit', 'N/A')}")
        print(f"Ocjena: {post['data'].get('score', 0)}")
        print(f"Vezani link: {post['data'].get('url', 'N/A')}")
        
        # Opcionalno: Ispis cijelog JSON-a radi proučavanja polja
        # print("\nPotpuni JSON:")
        # print(json.dumps(post, indent=2))
        
        # Potvrđivanje poruke je KLJUČNO
        message.ack()
        print("Poruka potvrđena (ACK).")
        
    except Exception as e:
        print(f" Greška pri obradi poruke: {e}")
        # U stvarnom svijetu, ovdje bi se mogla implementirati logika za NACK
        message.ack() # Za vježbu, uvijek potvrđujemo
        
def subscribe_to_topic():
    """Povezuje se s pretplatom i počinje slušati poruke."""
    
    if not PROJECT_ID or not SUBSCRIPTION_ID:
        print("Greška: Nedostaju GCP_PROJECT_ID ili PUBSUB_SUBSCRIPTION_ID environment varijable.")
        return

    print(f"Slušanje na pretplati: {SUBSCRIPTION_ID} (Projekt: {PROJECT_ID})")
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # Koristi se 'streaming pull'
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback
    )
    
    # Blokiranje glavne niti dok se ne prekine (npr. Ctrl+C)
    with subscriber:
        try:
            # Ovdje blokiramo zauvijek, čekajući poruke
            streaming_pull_future.result() 
        except KeyboardInterrupt:
            print("\n Prekid slušanja.")
            streaming_pull_future.cancel()
            streaming_pull_future.result()

if __name__ == "__main__":
    subscribe_to_topic()