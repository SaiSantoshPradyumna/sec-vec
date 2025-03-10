import time
import json
import base64
import random
from dotenv import load_dotenv  # Importing dotenv to load environment variables
import os
import secrets
import bleach
from datetime import datetime, timezone

from confluent_kafka import Producer
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


# Load environment variables
load_dotenv()

# Kafka Producer Configuration
# In this case, we specified that the Kafka broker is running locally on port 9092.
kafka_conf = {
    'bootstrap.servers': 'localhost:9092'  
}

# Initialize Kafka Producer
# Here, we initialize the Kafka producer using the configuration defined above. 
# This producer will allow us to send messages to a Kafka topic.
producer = Producer(kafka_conf)
aes_key_b64 = os.getenv('AES_KEY')
aes_iv_b64 = os.getenv('AES_IV')
# AES Encryption Setup
# We define the AES key and initialization vector (IV) for encryption.
# The AES key is 256 bits (32 bytes) long and the IV is 128 bits (16 bytes) long.
# These values will be used for encrypting event data using AES encryption.
AES_KEY = base64.b64decode(aes_key_b64)
AES_IV = base64.b64decode(aes_iv_b64)
def generate_and_store_key_iv(env_path='.env'):
    key_bytes = secrets.token_bytes(32)
    iv_bytes = secrets.token_bytes(16)
    key_b64 = base64.b64encode(key_bytes).decode('utf-8')
    iv_b64 = base64.b64encode(iv_bytes).decode('utf-8')
    with open(env_path, 'w', encoding='utf-8') as env_file:
        env_file.write(f"AES_KEY={key_b64}\n")
        env_file.write(f"AES_IV={iv_b64}\n")
        os.environ['AES_KEY'] = key_b64
        os.environ['AES_IV'] = iv_b64
# Event List
# This list contains possible events that our system simulates (e.g., "Speeding", "Brake Applied").
# These are the events that the producer will randomly pick and send to Kafka.
raw_events = os.getenv('EVENTS', '')
EVENTS = [bleach.clean(evt) for evt in raw_events.split(',') if evt.strip()]
# Encrypt Message Function
# This function encrypts a message using AES encryption in CBC (Cipher Block Chaining) mode.
# It pads the message to be compatible with the block size, encrypts it, and returns the encrypted data.
def encrypt_message(message, key, iv):
    # Initialize cipher for AES encryption in CBC mode
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Pad the message to be compatible with block size (128 bits for AES)
    padder = padding.PKCS7(128).padder()  # PKCS7 padding
    padded_data = padder.update(message.encode()) + padder.finalize()

    # Encrypt the padded message and return the encrypted data
    encrypted = encryptor.update(padded_data) + encryptor.finalize()
    return encrypted

# Produce Events Function
# This function simulates the production of car events.
# It continuously generates a random event from the list of possible events, adds a timestamp,
# encrypts the event, and sends it to a Kafka topic named 'car_events'. The producer then waits
# for a random period before sending the next event.
def produce_events():
    try:
        while True:
            # Randomly select an event and create a timestamp
            event = random.choice(EVENTS)
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')  # Get current UTC time

            # Prepare event data (event type and timestamp) as a dictionary
            event_data = {
                'event': event,
                'timestamp': timestamp
            }
            event_json = json.dumps(event_data)  # Serialize event data to JSON format

            # Encrypt the event message using AES encryption
            encrypted_message = encrypt_message(event_json, b'\x00' * 32, b'\x00' * 16)

            # Encode the encrypted message in base64 to ensure it can be sent as a string
            encoded_message = base64.b64encode(encrypted_message)

            # Produce (send) the encrypted and encoded event to the 'car_events' Kafka topic
            producer.produce('car_events', encoded_message)

            # Flush producer to ensure message is sent to Kafka
            producer.flush()

            # Print the event and timestamp for logging purposes
            print(f"Produced event: {event_data['event']} at {event_data['timestamp']}")

            # Wait for a random time interval before producing the next event
            time.sleep(random.uniform(0.5, 2.0))  # Random wait between 0.5 and 2.0 seconds

    except KeyboardInterrupt:
        # typically ctrl +c which i use
        print("Event production stopped.")

# Main Entry Point
# This ensures that the event production process starts when the script is run directly (not imported).
if __name__ == "__main__":
    produce_events()