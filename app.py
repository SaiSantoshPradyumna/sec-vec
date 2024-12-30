from confluent_kafka import Producer, Consumer, KafkaError

# Kafka configuration
BROKER = "localhost:9092"  # Change this to your Kafka broker address
TOPIC = "test-topic"       # Replace with your desired topic name

# Function to produce messages
def produce_messages():
    conf = {'bootstrap.servers': BROKER}
    producer = Producer(conf)

    def delivery_report(err, msg):
        """Callback for message delivery confirmation."""
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Producing 10 test messages
    for i in range(10):
        message = f"Message {i}"
        producer.produce(TOPIC, value=message, callback=delivery_report)
        producer.flush()  # Ensure the message is sent to Kafka

    print("All messages sent!")

# Function to consume messages
def consume_messages():
    conf = {
        'bootstrap.servers': BROKER,
        'group.id': 'my-group',          # Consumer group ID
        'auto.offset.reset': 'earliest'  # Start consuming from the earliest message
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print("Consuming messages...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Wait for a message
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"End of partition: {msg.topic()} [{msg.partition()}]")
                elif msg.error():
                    print(f"Error: {msg.error()}")
                    break
            else:
                # Successfully received a message
                print(f"Received message: {msg.value().decode('utf-8')} from {msg.topic()} [{msg.partition()}]")

    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("1. Produce messages")
    print("2. Consume messages")
    choice = input("Enter choice (1 or 2): ")

    if choice == "1":
        produce_messages()
    elif choice == "2":
        consume_messages()
    else:
        print("Invalid choice!")
