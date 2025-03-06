import os
import json
import base64
import psycopg2
from psycopg2 import sql
from datetime import datetime
from confluent_kafka import Consumer,KafkaException
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
from cryptography.hazmat.backends import default_backend

db_config={"host":os.getenv("DB_HOST"),"database":os.getenv("DB_NAME"),"user":os.getenv("DB_USER"),"password":os.getenv("DB_PASSWORD")}
kafka_conf={"bootstrap.servers":"localhost:9092","group.id":"car_event_consumers","auto.offset.reset":"earliest"}
consumer=Consumer(kafka_conf)
consumer.subscribe(["car_events"])
AES_KEY=base64.b64decode(os.getenv("AES_KEY"))
AES_IV=base64.b64decode(os.getenv("AES_IV"))

def decrypt_message(m,k,i):
    c=Cipher(algorithms.AES(k),modes.CBC(i),backend=default_backend()).decryptor()
    d=c.update(m)+c.finalize()
    u=padding.PKCS7(128).unpadder()
    return (u.update(d)+u.finalize()).decode()

def store_event(d):
    conn=None
    try:
        conn=psycopg2.connect(**db_config)
        cur=conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS car_event_logs (id SERIAL PRIMARY KEY, event VARCHAR(50), timestamp TIMESTAMP)")
        cur.execute("INSERT INTO car_event_logs (event,timestamp) VALUES (%s,%s)",(d["event"],d["timestamp"]))
        conn.commit()
        ft=datetime.fromisoformat(d["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Stored event: {d['event']} at {ft}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def consume_events():
    try:
        while True:
            msg=consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            dm=decrypt_message(base64.b64decode(msg.value()),AES_KEY,AES_IV)
            store_event(json.loads(dm))
    except KeyboardInterrupt:
        print("Stopped")
    finally:
        consumer.close()

if __name__=="__main__":
    consume_events()
