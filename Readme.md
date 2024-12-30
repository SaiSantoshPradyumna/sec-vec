# Car Event Streaming and Logging System

## Project Overview
This project simulates a pipeline for generating, processing, and storing car events such as "Speeding" or "Brake Applied". The full implementation includes the following features:

## Full Project Overview

### Event Generation
- Randomly selects events from a predefined list (e.g., "Speeding", "Brake Applied", "Lane Departure").
- Appends a timestamp to each event.

### Data Streaming
- Streams events to a Kafka topic using the Kafka Python client.
- Encrypts event messages using AES encryption for security.
- Encodes encrypted messages using Base64.

### Data Consumption and Storage
- Consumes events from the Kafka topic.
- Decrypts and decodes the messages.
- Stores decrypted event logs (including timestamps) into a PostgreSQL database.

## Current Implementation

### Implemented Features

- **Event Generation**: The current implementation focuses on generating car events and appending timestamps. It outputs these events to the console in real-time.

### Features Yet to Be Implemented

1. Data streaming via Kafka.
2. AES encryption for securing event messages.
3. Storing event logs in a PostgreSQL database.

## Prerequisites

- Python 3.7 or higher.

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   ```
2. Navigate to the project directory:
   ```bash
   cd <project-directory>
   ```
3. Install any required dependencies (none for the current implementation).

## Usage

1. Run the script:
   ```bash
   python <script-name>.py
   ```
2. The program will generate random events and print them to the console in real-time.

3. Stop the program using `Ctrl+C`.

## Example Output

```
Generated Event: {'event': 'Speeding', 'timestamp': '2024-12-30T12:00:00.123456'}
Generated Event: {'event': 'Brake Applied', 'timestamp': '2024-12-30T12:00:01.123456'}
Generated Event: {'event': 'Lane Departure', 'timestamp': '2024-12-30T12:00:02.123456'}
```

## Future Enhancements

- Implement Kafka integration for data streaming.
- Add AES encryption for securing event messages.
- Develop PostgreSQL database interaction for event storage.

