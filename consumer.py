import os
from dotenv import load_dotenv
import json
import base64
import psycopg2
from psycopg2 import sql
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

# Load environmet variabless
load_dotenv()

# Database Configuraton
# This dictonary contains the databse host, name, user, and password fetched from environment variables.
db_config = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Kafka Configurattion
# Kafka consumer configuration includess bootstrap server, group id and offset reset.
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'car_event_consumers',
    'auto.offset.reset': 'earliest'
}

# Initialize the Kafka consumer and subscribe to the "car_events" topic
consumer = Consumer(kafka_conf)
consumer.subscribe(['car_events'])

# AES Key and IV Setup
# The AES Key and IV are used to decrypt the incomming messages.
AES_KEY = b'\x00' * 32
AES_IV = b'\x00' * 16

# Decrypts encrypted messages using AES CBC mode
# This functon decrypts a given encrypted message and removes paddding.
def decrypt_message(encrypted_message, key, iv):
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    decrypted_padded = decryptor.update(encrypted_message) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()
    return decrypted.decode()

# Store the event into the databse
# This function stores the event data into a PostgreSQL databse after ensuring the table exists.
def store_event(event_data):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS car_event_logs (
                id SERIAL PRIMARY KEY,
                event VARCHAR(50),
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

        # Formatt the timestamp before printing
        formatted_timestamp = datetime.fromisoformat(event_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Stored event: {event_data['event']} at {formatted_timestamp}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while storing event: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

# Consume Kafka events and process them
# This function polls the Kafka topic for messages, decrypts them, and stores them in the database.
def consume_events():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            encrypted_message = base64.b64decode(msg.value())
            decrypted_message = decrypt_message(encrypted_message, AES_KEY, AES_IV)
            event_data = json.loads(decrypted_message)
            store_event(event_data)
    except KeyboardInterrupt:
        print("Event consumption stopped.")
    finally:
        consumer.close()

# Main entry point for the consumer script
# This ensures the consume_events functin is called when the script is run directly.
if __name__ == "__main__":
    consume_events()
