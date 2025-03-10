
import os
import base64
import json
import psycopg2
from datetime import datetime
from psycopg2 import sql
from confluent_kafka import Consumer, KafkaException

from dotenv import load_dotenv
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def decrypt_message(encrypted_bytes, key, iv):
    """
    Decrypt AES-CBC-encrypted bytes with PKCS7 padding.
    Returns the decrypted string.
    Throws ValueError if padding is invalid.
    """
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()

    decrypted_padded = decryptor.update(encrypted_bytes) + decryptor.finalize()

    unpadder = padding.PKCS7(128).unpadder()
    decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()
    return decrypted.decode()

def try_decrypt_with_two_keys(encrypted_bytes, old_key, old_iv, new_key, new_iv):
    """
    Attempt to decrypt with old key/IV first. If that fails with padding error,
    try new key/IV. Raise an exception if both fail.
    """
    # First attempt: old key
    try:
        return decrypt_message(encrypted_bytes, old_key, old_iv)
    except ValueError:
        # Possibly invalid padding => try new key
        pass

    # Second attempt: new key
    try:
        return decrypt_message(encrypted_bytes, new_key, new_iv)
    except ValueError:
        # Both attempts failed => re-raise
        raise ValueError("Unable to decrypt with either old or new key.")

def store_event(event_data, db_config):
    """
    Store event data into PostgreSQL, auto-creating table if needed.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        create_table_query = '''
            CREATE TABLE IF NOT EXISTS car_event_logs (
                id SERIAL PRIMARY KEY,
                event VARCHAR(100),
                timestamp TIMESTAMP
            );
        '''
        cursor.execute(create_table_query)

        insert_query = '''
            INSERT INTO car_event_logs (event, timestamp)
            VALUES (%s, %s);
        '''
        cursor.execute(insert_query, (event_data['event'], event_data['timestamp']))
        conn.commit()

        dt_obj = datetime.fromisoformat(event_data['timestamp'])
        print(f"Stored event: {event_data['event']} at {dt_obj}")
    except Exception as e:
        print(f"Error while storing event: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def consume_events(env_path='.env'):
    """
    Continuously poll messages from 'car_events' and try to decrypt them.
    Tries "OLD" key first, then "NEW" key.
    """
    # Load .env (which should have OLD_AES_KEY/IV, NEW_AES_KEY/IV)
    load_dotenv(env_path)

    old_key_b64 = os.getenv('OLD_AES_KEY', '')
    old_iv_b64  = os.getenv('OLD_AES_IV', '')
    new_key_b64 = os.getenv('NEW_AES_KEY', '')
    new_iv_b64  = os.getenv('NEW_AES_IV', '')

    if any(not x for x in [old_key_b64, old_iv_b64, new_key_b64, new_iv_b64]):
        print("One of OLD_AES_KEY/IV or NEW_AES_KEY/IV is missing.")
        return

    old_key = base64.b64decode(old_key_b64)
    old_iv  = base64.b64decode(old_iv_b64)
    new_key = base64.b64decode(new_key_b64)
    new_iv  = base64.b64decode(new_iv_b64)

    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    kafka_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'car_event_consumers',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['car_events'])

    print("Starting consumer... Press Ctrl+C to exit.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            encrypted_b64 = msg.value()  # base64-encoded ciphertext
            if encrypted_b64 is None:
                continue

            encrypted_bytes = base64.b64decode(encrypted_b64)

            # Try decrypt with old key, then new
            try:
                decrypted_json = try_decrypt_with_two_keys(
                    encrypted_bytes,
                    old_key, old_iv,
                    new_key, new_iv
                )
            except ValueError:
                print("Failed to decrypt message with both old/new keys.")
                continue

            # Parse JSON
            try:
                event_data = json.loads(decrypted_json)
            except json.JSONDecodeError:
                print("Decrypted message is not valid JSON.")
                continue

            # Store to DB
            store_event(event_data, db_config)

    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_events('.env')

