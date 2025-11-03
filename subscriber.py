import base64
import json
import os
import time
from flask import Flask, request, jsonify

# Flask application instance
app = Flask(__name__)

# Cloud Run requires binding to 0.0.0.0 and using the PORT environment variable
PORT = int(os.environ.get("PORT", 8080))

@app.route("/", methods=["POST"])
def index():
    """
    Receives Pub/Sub push messages via an HTTP POST request.
    
    The message is in a JSON wrapper (the "envelope") in the request body.
    
    :return: An HTTP response status code to acknowledge (200) or reject (non-2xx).
    """
    
    # Check for the JSON data in the request body
    envelope = request.get_json()
    if not envelope:
        # Bad request: no JSON payload
        print("ERROR: No JSON payload found in request.")
        return ("Bad Request: No JSON payload", 400)

    # 1. Extract the Pub/Sub message from the envelope
    try:
        message_data = envelope["message"]
        message_id = message_data.get("message_id")
        data_b64 = message_data.get("data")
        attributes = message_data.get("attributes", {})
    except KeyError:
        print(f"ERROR: Invalid Pub/Sub message format in envelope: {envelope}")
        return ("Invalid Pub/Sub message format", 400)

    # 2. Decode and Process the Message Data
    try:
        # The 'data' field is base64-encoded
        if data_b64 is None:
            data_string = "{}" # Treat missing data as an empty JSON object
        else:
            data_bytes = base64.b64decode(data_b64)
            data_string = data_bytes.decode("utf-8")
        
        # Parse the JSON string
        data_json = json.loads(data_string)

        # --- Your original processing logic ---
        print("-" * 50)
        print(f"[{time.strftime('%H:%M:%S')}] Received Message ID: {message_id}")
        print(f"JSON Data:\n{json.dumps(data_json, indent=2)}")
        
        if attributes:
            print(f"Attributes: {attributes}")
        
        # NOTE: Acknowledgment in Push Subscriptions is done by returning a 2xx HTTP status code.
        # We implicitly acknowledge by reaching the return statement.
        print("Message Processed and Implicitly Acknowledged (200 OK).")
        # --------------------------------------

        # Acknowledge the message by returning an HTTP 200 OK
        return ("", 200)

    except json.JSONDecodeError as e:
        print(f"ERROR: Could not decode message data as JSON. Error: {e}")
        print(f"Raw Data (string): {data_string}")
        # Return an error status (e.g., 500) to tell Pub/Sub to retry
        return (f"Internal Server Error: JSON Decode Error: {e}", 500)
        
    except Exception as e:
        print(f"An unexpected error occurred during message processing: {e}")
        # Return an error status to tell Pub/Sub to retry
        return (f"Internal Server Error: {e}", 500)


if __name__ == "__main__":
    # When running locally, set debug=True
    app.run(debug=True, host="0.0.0.0", port=PORT)