# Car Event Streaming and Logging System

## Project Overview

This project simulates a car's event generation system and securely streams the events over Apache Kafka. The events are encrypted using AES encryption to maintain confidentiality and are base64 encoded for consistent message size. A Python consumer listens to the Kafka topic, decrypts and decodes the messages, and stores the event logs into a PostgreSQL database.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Description](#project-description)
- [Tools and Technologies](#tools-and-technologies)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
- [Running the Project](#running-the-project)
- [Notes and Recommendations](#notes-and-recommendations)
- [License](#license)

## Project Description

The project consists of two main components:

1. **Event Generation and Streaming (`producer.py`):**

   - Simulates car events by randomly selecting from a predefined list of events such as "Speeding", "Brake Applied", "Lane Departure", etc.
   - Each event is appended with the current timestamp in UTC.
   - The event messages are encrypted using AES encryption with a shared key and initialization vector (IV) for security.
   - The encrypted messages are encoded using Base64 to ensure consistent message size.
   - The encoded messages are sent to a Kafka topic (`car_events`).

2. **Data Consumption and Storage (`consumer.py`):**

   - Subscribes to the Kafka topic (`car_events`) and listens for incoming encrypted event messages.
   - Decrypts the messages using the shared AES key and IV after decoding them from Base64.
   - Parses the event data and stores the decrypted event logs (including timestamps) into a PostgreSQL database (`car_events`).

## Tools and Technologies

The project uses the following tools and technologies:

- **Programming Language:**
  - Python 3.x

- **Libraries and Packages:**
  - [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python): For Kafka integration.
  - [`cryptography`](https://cryptography.io/en/latest/): For AES encryption and decryption.
  - [`psycopg2`](https://www.psycopg.org/): For PostgreSQL database interactions.

- **Services:**
  - **Apache Kafka:** A distributed streaming platform for real-time data pipelines.
  - **PostgreSQL:** An open-source relational database management system.

- **Additional Requirements:**
  - A running **Kafka broker**.
  - A **PostgreSQL** database server with access credentials.

Apologies for the confusion earlier! Here's the corrected format for your project structure to add to your `README.md` file:


## Project Structure

The project files are organized as follows:

```markdown
car-event-streaming/
│
├── producer.py         # Producer script for event generation and streaming
├── consumer.py         # Consumer script for data consumption and storage
├── requirements.txt    # Python dependencies required by the project
├── .env                # Environmental Variables required by the project
└── README.md           # Project documentation
```


### Explanation of files:

- `producer.py`: The script responsible for generating and streaming events (e.g., car data).
- `consumer.py`: The script that consumes and stores the generated events.
- `requirements.txt`: A file containing the list of Python dependencies required for the project.
- `README.md`: This documentation file.
- **`.env`**:  This file contains environment-specific configuration variables, such as database credentials, and EVENTS in producer.py.



# Kafka Setup and Configuration Process

This below are the steps you executed and explains the purpose and reasoning behind each command.

---

### 1. Generating a Random UUID for Kafka Storage

```powershell
& "C:\kafka\bin\windows\kafka-storage.bat" random-uuid
```

### 2. Checking the Kafka Cluster ID

```powershell
"C:\kafka\bin\windows\kafka-storage.bat" <the cluster id returned in the prev command> --bootstrap-server localhost:9092
```


### 3.  Formatting Kafka Storage

```powershell
& "C:\kafka\bin\windows\kafka-storage.bat" format -t <the cluster id returned in the prev command> -c "C:\kafka\config\kraft\server.properties"
```

### 4. Starting Kafka Broker

```powershell
& "C:\kafka\bin\windows\kafka-server-start.bat" "C:\kafka\config\kraft\broker.properties"
```

## Summary

The commands you executed follow the necessary steps to set up and configure a Kafka broker using KRaft mode. Here's a summary of the process:

1. **Generate a random UUID** for Kafka's internal metadata storage.
2. **Attempt to check the cluster ID**, but encountered an invalid command.
3. **Format the Kafka metadata storage**, initializing it with the generated UUID and configuration.
4. **Start the Kafka broker** using the provided `server.properties` configuration.

## Setup and Installation

Follow these steps to set up and run the project on your local machine.

### Prerequisites

- **Python 3.x** installed on your system.
- **Apache Kafka** installed and running.
- **PostgreSQL** database server installed and running.
- **Git** (optional, for cloning the repository).

### 1. Clone the Repository

Clone the project repository from GitHub (replace `<repository-url>` with your repository URL):

```bash
git clone <repository-url>
cd sec-vec
```

### 2. Create a Virtual Environment (Recommended)
Use a virtual environment to manage project dependencies:

```bash
# For Windows
# Furst Initialize it
python -m venv venv
# Then Activate it
venv\Scripts\activate
```
### 3. Install Python Dependencies
Install the required Python packages using the requirements.txt file:
```bash
pip install -r requirements.txt
```
Certainly! Here's the same information converted into Markdown format:

```markdown
## 4. Set Up PostgreSQL Database

You will need to create a PostgreSQL database and table to store the car events.

### 4.1 Create the Database

Log into your PostgreSQL database server. For example, you can use `psql` from the command line or connect through a database client.

```bash
psql -U <username> -h <hostname> -W
```

Once logged in, create the `car_events` database with the following SQL command:

```sql
CREATE DATABASE car_events;
```

### 4.2 Configure `.env` File

Make sure to set up the environment variables to include the PostgreSQL connection details. You can create a `.env` file in the project root directory with the following content:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=car_events
DB_USER=your_postgresql_username
DB_PASSWORD=your_postgresql_password
```


# Running the Project
Follow these steps to run the producer and consumer scripts.

### 1. Run the Producer Script (producer.py)
Open a terminal window, navigate to the project directory, and start the producer script:
```bash
python producer.py
```
The producer will start generating events, encrypting them, encoding them with Base64, and sending them to the Kafka topic car_events.

You should see output similar to:
```bash
Produced event: Speeding at 2024-12-29 21:02:01
Produced event: Brake Applied at 2024-12-29 21:02:04
```
### 2. Run the Consumer Script (consumer.py)
Open another terminal window, navigate to the project directory, and start the consumer script:
```bash
python consumer.py
```
The consumer will start listening to the Kafka topic car_events, decrypting and decoding the messages, and storing the events into the PostgreSQL database.

You should see output similar to:
```bash
Stoered event: Speeding at 2024-12-29 21:02:01
Stoered event: Brake Applied at 2024-12-29 21:02:04
```


### 3. Verify Data Storage
You can verify that the events are stored in the PostgreSQL database by querying the car_event_logs table:
```bash
SELECT * FROM car_event_logs;
```


