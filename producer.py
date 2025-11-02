
import requests
import json
import time
from google.cloud import pubsub_v1


PROJECT_ID = "tpiuo-lab1"
TOPIC_ID = "projects/tpiuo-lab1/topics/reddit-topic-0036565755"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

REDDIT_URL = "https://www.reddit.com/r/dataengineering/top/.json?t=all&limit=10"

headers = {
    'User-Agent': 'tpiuolab1/1.0'
}

response = requests.get(REDDIT_URL, headers=headers)
data = response.json()
posts = data['data']['children'] 
for post in posts:
    data_string = json.dumps(post['data'])
    data_bytes = data_string.encode("utf-8")
    future = publisher.publish(topic_path, data_bytes)
    print(f"Poslana objava ID: {future.result()}")
while True:
    time.sleep(3600)