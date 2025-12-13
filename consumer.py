from flask import Flask, request, jsonify
import os
import json
import base64

app = Flask(__name__)
# Cloud Run automatski postavlja PORT varijablu
PORT = int(os.environ.get('PORT', 8080))

@app.route('/listening', methods=['GET'])
def listening_check():
    """Jednostavna ruta za provjeru je li servis aktivan."""
    return 'Consumer service is listening', 200

@app.route('/', methods=['POST'])
def pubsub_receiver():
    """Prima Pub/Sub poruke poslane kao HTTP POST request."""
    
    # 1. Čitanje wrapper objekta koji šalje Pub/Sub
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        print("Greška: Neispravan Pub/Sub format poruke.")
        return 'Not a valid Pub/Sub message', 400

    message = envelope['message']
    
    if 'data' not in message:
        print("Greška: Poruka ne sadrži podatke (data polje).")
        # Pub/Sub poruke bez podataka (data) su rijetke, ali moguće
        return 'No data field in message', 200 # Ipak vraćamo 200 da ne re-trya

    try:
        # 2. Dekodiranje Base64 podataka
        pubsub_data = base64.b64decode(message['data']).decode('utf-8')
        # 3. Parsiranje JSON-a
        reddit_post = json.loads(pubsub_data)
        
        # 4. OBRADA PODATAKA
        title = reddit_post.get('data', {}).get('title', 'N/A')
        score = reddit_post.get('data', {}).get('score', 0)
        
        print(f"🎉 Primljena objava (ID: {message.get('message_id')})")
        print(f"Naslov: {title}")
        print(f"Ocjena: {score}")
        
        # KLJUČNO: Vratiti 200 da Pub/Sub zna da je poruka uspješno obrađena
        return 'OK', 200
    
    except Exception as e:
        print(f"Greška pri obradi: {e}")
        # U slučaju greške u obradi, vraćanje 500 će uzrokovati da Pub/Sub ponovo pokuša
        return f'Error processing: {e}', 500

if __name__ == "__main__":
    print(f"Starting Consumer Service on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)