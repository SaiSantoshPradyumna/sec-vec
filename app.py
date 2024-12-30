import random
import time
from datetime import datetime

# Event Generation
EVENTS = ["Speeding", "Brake Applied", "Lane Departure", "Hard Acceleration", "Sharp Turn"]

def generate_event():
    event = random.choice(EVENTS)
    timestamp = datetime.now().isoformat()
    return {"event": event, "timestamp": timestamp}

def main():
    try:
        while True:
            event = generate_event()
            print(f"Generated Event: {event}")
            time.sleep(1)  # Simulate real-time event generation
    except KeyboardInterrupt:
        print("Event generation stopped.")

if __name__ == "__main__":
    main()
