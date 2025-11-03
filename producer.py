import os
import time
import json
import requests
from google.cloud import pubsub_v1


PROJECT_ID = "tpiuo-lab1" 
TOPIC_ID = "reddit-topic-0036565755" 

SUBREDDIT = "dataengineering"
LIMIT = 10
TIME_FILTER = "all"
USER_AGENT = "script:data-engineering-fetcher:v1.0 (by /u/mrsomeawe)" 


def load_reddit_credentials():

    # Naziv okolišne varijable na koju je Secret Manager mapiran
    api_key = os.getenv("API_KEY") 

    if not api_key:
        raise ValueError("nije pronadeno")

    try:
        creds = json.loads(api_key)
        required_keys = ["client_id", "client_secret", "username", "password"]
        if not all(key in creds for key in required_keys):
            raise KeyError(f"JSON tajna ne sadrži sve potrebne ključeve: {required_keys}")
            
        return creds
        
    except json.JSONDecodeError:
        raise ValueError("Greška prilikom parsiranja JSON tajne.")
    except KeyError as e:
        raise e


def get_reddit_access_token(creds):
    """
    Izvršava OAuth2 'Password Grant' tok za dobivanje pristupnog tokena.
    """
    
    token_url = "https://www.reddit.com/api/v1/access_token"
    
    auth = requests.auth.HTTPBasicAuth(creds['client_id'], creds['client_secret'])
    
    data = {
        'grant_type': 'password',
        'username': creds['username'],
        'password': creds['password']
    }
    
    headers = {'User-Agent': USER_AGENT}
    
    try:
        response = requests.post(token_url, auth=auth, data=data, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        
        if 'access_token' in token_data:
            return token_data['access_token']
        else:
            raise Exception("Nije pronađen 'access_token' u odgovoru.")
            
    except requests.exceptions.RequestException as e:
        print(f"Greška prilikom dohvaćanja Access Tokena: {e}")
        return None

def fetch_top_posts(access_token):
    api_url = f"https://oauth.reddit.com/r/{SUBREDDIT}/top"
    
    headers = {
        "Authorization": f"bearer {access_token}",
        "User-Agent": USER_AGENT
    }
    params = {
        "limit": LIMIT,
        "t": TIME_FILTER
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        posts = data['data']['children']
        return posts
        
    except requests.exceptions.RequestException as e:
        print(f"Greška prilikom dohvaćanja objava: {e}")
        return []


def publish_posts_to_pubsub(posts):

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    except Exception as e:
        print(f"Greška kod inicijalizacije Pub/Sub klijenta: {e}")
        return

    for post in posts:
        post_data = post['data']
        
        message_json = json.dumps(post_data)
        message_bytes = message_json.encode("utf-8")
        
        future = publisher.publish(topic_path, data=message_bytes)
        
        try:
            message_id = future.result()
        except Exception as e:
            print(f"Greška prilikom objavljivanja: {e}")
            

if __name__ == "__main__":
    try:
        reddit_creds = load_reddit_credentials()

        reddit_token = get_reddit_access_token(reddit_creds)
        
        if reddit_token:
            posts_to_publish = fetch_top_posts(reddit_token)
            
            if posts_to_publish:
                publish_posts_to_pubsub(posts_to_publish)
            else:
                print("Nije moguće dohvatiti objave. Prekid programa.")
        else:
            print("Autentifikacija na Reddit API nije uspjela. Prekid programa.")

    except (ValueError, KeyError, Exception) as e:
        print(f"FATALNA GREŠKA: {e}")
        

    print("\nUlazak u beskonačnu petlju (za održavanje Cloud Run/Docker kontejnera aktivnim). Pritisnite Ctrl+C za izlaz.")
    while True:
        try:
            time.sleep(1) 
        except KeyboardInterrupt:
            print("\nPrekinuto od strane korisnika.")
            break