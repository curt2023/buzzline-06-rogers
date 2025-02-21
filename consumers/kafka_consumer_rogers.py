"""
kafka_consumer_rogers.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "title" : "Python, the Rise of code"
    "review": "I wish that I could get my money back"
    "critic": "Bob"
    "timestamp": "2025-02-20 07:53:22"
    "genre": "Comedy"
    "sentiment": 0.38
    "message_length":37
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib import colors as mcolors



# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_rogers import init_db, insert_message

fig = plt.figure(figsize=(10,8))
fig.patch.set_facecolor('slategrey')
gs = gridspec.GridSpec(2, 2, height_ratios=[1,1])
plt.ion()


DB_PATH = config.get_sqlite_path()

def fetch_data():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT genre, avg_sentiment FROM sentiment_per_genre")
            visual_data1 = cursor.fetchall()
        return visual_data1
    except Exception as e:
        logger.error(f"Error Fetching data: {e}")

  
def update_chart():
    while True:
        visual_data1, critic_data = fetch_data() 
 

        ax1 = fig.add_subplot(gs[0,0])
        ax1.clear() #clear previous chart

        genre, avg_sentiment = zip(*visual_data1)


        ax.bar(genre, avg_sentiment, color="lawngreen", edgecolor ='orange')
        ax.set_title("Average Sentiment per Category")
        ax.set_ylabel("avg_sentiment")
        ax.set_xlabel("genre")
        ax.set_facecolor("lightsteelblue")
        ax.set_ylim(0,1)

        plt.tight_layout()
        plt.draw()
        plt.pause(2)

   

 

#####################################
# Function to process a single message
# #####################################


def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    try:
        processed_message = {
            "title": message.get("title"),
            "review": message.get("review"),
            "critic": message.get("critic"),
            "timestamp": message.get("timestamp"),
            "genre": message.get("genre"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
        #insert_message(processed_message, DB_PATH)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None
    


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")

    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, DB_PATH)
    
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")

    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()

        init_db(DB_PATH)

        import threading
        consumer_thread = threading.Thread(target= consume_messages_from_kafka, args=(topic,kafka_url,group_id))
        consumer_thread.daemon = True
        consumer_thread.start()

        update_chart()

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

   

#####################################
# Conditional Execution
#####################################


if __name__ == "__main__":
    main()