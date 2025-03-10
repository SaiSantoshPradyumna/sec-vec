
import os
import base64
import secrets
import json
import time
import random
import bleach
from datetime import datetime, timezone

from dotenv import load_dotenv, dotenv_values
from confluent_kafka import Producer
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def rotate_keys_in_env(env_path='.env'):
    """
    1) Read existing values from .env if present.
    2) Rename NEW_AES_KEY/IV --> OLD_AES_KEY/IV.
    3) Generate fresh NEW_AES_KEY/IV.
    4) Save all four keys (OLD + NEW) back to .env.
    """
    # Load the existing .env into memory
    if os.path.exists(env_path):
        existing_vars = dotenv_values(env_path)
    else:
        existing_vars = {}

    old_key_b64 = existing_vars.get('OLD_AES_KEY', '')
    old_iv_b64 = existing_vars.get('OLD_AES_IV', '')
    new_key_b64 = existing_vars.get('NEW_AES_KEY', '')
    new_iv_b64 = existing_vars.get('NEW_AES_IV', '')

    # Rotate: move NEW -> OLD
    existing_vars['OLD_AES_KEY'] = new_key_b64 or old_key_b64
    existing_vars['OLD_AES_IV'] = new_iv_b64 or old_iv_b64

    # Generate brand-new "NEW" key/IV
    key_bytes = secrets.token_bytes(32)  # 256-bit
    iv_bytes = secrets.token_bytes(16)   # 128-bit
    new_key_b64 = base64.b64encode(key_bytes).decode('utf-8')
    new_iv_b64 = base64.b64encode(iv_bytes).decode('utf-8')

    existing_vars['NEW_AES_KEY'] = new_key_b64
    existing_vars['NEW_AES_IV'] = new_iv_b64

    # Write updated values back to .env
    with open(env_path, 'w', encoding='utf-8') as env_file:
        for key, val in existing_vars.items():
            env_file.write(f"{key}={val}\n")

    # Also set them in our process environment
    os.environ['OLD_AES_KEY'] = existing_vars['OLD_AES_KEY']
    os.environ['OLD_AES_IV'] = existing_vars['OLD_AES_IV']
    os.environ['NEW_AES_KEY'] = existing_vars['NEW_AES_KEY']
    os.environ['NEW_AES_IV'] = existing_vars['NEW_AES_IV']


def encrypt_message(message, key, iv):
    """
    Encrypts a message (string) using AES-CBC with PKCS7 padding.
    Returns the encrypted bytes.
    """
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(message.encode()) + padder.finalize()

    encrypted = encryptor.update(padded_data) + encryptor.finalize()
    return encrypted

def produce_events(env_path='.env'):
    """
    Produces random events to the 'car_events' topic, using the "NEW" key from the .env file.
    """
    # Load environment
    load_dotenv(env_path)

    # Get newly created "NEW" key
    aes_key_b64 = os.getenv('NEW_AES_KEY', '')
    aes_iv_b64 = os.getenv('NEW_AES_IV', '')

    if not aes_key_b64 or not aes_iv_b64:
        print("NEW_AES_KEY or NEW_AES_IV not found in environment.")
        return

    aes_key = base64.b64decode(aes_key_b64)
    aes_iv = base64.b64decode(aes_iv_b64)

    # Kafka config
    kafka_conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    producer = Producer(kafka_conf)

    # Load possible events (or use a default)
    raw_events = os.getenv('EVENTS', 'Speeding,Brake Applied,Battery Low')
    events = [bleach.clean(evt.strip()) for evt in raw_events.split(',') if evt.strip()]

    try:
        while True:
            event = random.choice(events)
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            event_dict = {
                'event': event,
                'timestamp': timestamp
            }
            event_json = json.dumps(event_dict)

            # Encrypt with the "NEW" key
            encrypted_bytes = encrypt_message(event_json, aes_key, aes_iv)
            encoded_str = base64.b64encode(encrypted_bytes)

            producer.produce('car_events', encoded_str)
            producer.flush()

            print(f"Produced event: {event} at {timestamp}")
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("Producer stopped.")

if __name__ == "__main__":
    # 1) Rotate keys (NEW -> OLD and then generate fresh NEW)
    rotate_keys_in_env('.env')
    # 2) Start producer
    produce_events('.env')
