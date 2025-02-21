"""
producer_rogers.py

Stream JSON data to a file and - if available - a Kafka topic.

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

Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################


def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)


#####################################
# Define Message Generator
#####################################


def generate_messages():
    """
    Generate a stream of JSON messages.
    """
    TITLE_INTRO = ["Python, ",
             "Untouchable:",
             "Clear as Mud: ",
             "Stronger, "
             ]
    TITLE_END = ["Masters of the Code", 
              "the Rise of Code", 
              "an Underdog Story", 
              "A kafka Story"
              ]
    GENRE = ["Comdey", "Action", "Romance", "Sci-Fi"]
    REVIEW = ["This was the best movie I have seen",
              "This movie had me laughing from start to end",
              "Horrible film",
              "Would watch again",
              "Was a complete waste of time",
              "Great story",
              "Movie of the YEAR",
              "I wish that I could get my money back",
              "Life changing",
              "Two thumbs way down",
              "two thumbs way up"]
    CRITICS = ["Frank", "Bob", "Charlie", "Eve", "Sally", "George", "Tilly"]
    STARS = [1,3,4,5]

    while True:
        title_intro = random.choice(TITLE_INTRO)
        title_end = random.choice(TITLE_END)
        genre =random.choice(GENRE)
        critic = random.choice(CRITICS)
        title = f"{title_intro} {title_end}"
        review = random.choice(REVIEW)
        #stars = random.choice(STARS)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # Assess sentiment
        sentiment = assess_sentiment(review)

        # Create JSON message
        json_message = {
            "title": title,
            "review": review,
            #"stars": stars,
            "critic": critic,
            "timestamp": timestamp,
            "genre": genre,
            "sentiment": sentiment,
            "message_length": len(review),
        }

        yield json_message


#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
