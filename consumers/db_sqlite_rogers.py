""" db_sqlite_rogers.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

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

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages")
            cursor.execute("DROP TABLE IF EXISTS tilly_sentiment")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    review TEXT,
                    critic TEXT,
                    timestamp TEXT,
                    genre TEXT,
                    sentiment REAL,
                    message_length INTEGER
                )
            """
            )

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_per_genre (
                genre TEXT PRIMARY KEY,
                avg_sentiment REAL
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS critic_entry_counts (
                critic TEXT PRIMARY KEY,
                review_count REAL
            )
            """)


            cursor.execute("""
            CREATE TABLE IF NOT EXISTS tilly_sentiment (
                critic TEXT,
                timestamp DATE,
                genre TEXT,
                sentiment REAL
            )
            """)

            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")


#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages(
                    title,review, critic, timestamp, genre, sentiment, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message["title"],
                    message["review"],
                    message["critic"],
                    message["timestamp"],
                    message["genre"],
                    message["sentiment"],
                    message["message_length"],
                ),
            )

                        # Update category sentiment (calculate average)
            cursor.execute(
                """
                INSERT INTO sentiment_per_genre (genre, avg_sentiment)
                VALUES (?, ?)
                ON CONFLICT(genre) DO UPDATE SET avg_sentiment = (
                    SELECT AVG(sentiment) FROM streamed_messages WHERE genre = ?
                )
            """, (message["genre"],
                  message["sentiment"],
                  message["genre"],
                ))
            
            cursor.execute(
                """
                INSERT INTO critic_entry_counts (critic, review_count)
                VALUES (?, ?)
                ON CONFLICT(critic) DO UPDATE SET review_count = (
                    SELECT COUNT(title) FROM streamed_messages WHERE critic = ?
                )
            """, (message["critic"],
                  message["title"],
                  message["critic"],
                ))
            
            cursor.execute(
                """
                INSERT INTO tilly_sentiment(
                    critic, timestamp, genre, sentiment
                ) VALUES (?, ?, ?, ?)
            """,
                (
                    message["critic"],
                    message["timestamp"],
                    message["genre"],
                    message["sentiment"]
                ),
            )
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")


#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.path = config.get_base_data_path
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    test_message = {
        "title": "Just a test title",
        "message": "I just shared a meme! It was amazing.",
        "critic": "Charlie",
        "timestamp": "2025-01-29 14:35:20",
        "genre": "comedy",
        "sentiment": 0.87,
        "message_length": 42,
    }

    insert_message(test_message, TEST_DB_PATH)

    # Retrieve the ID of the inserted test message
    try:
        with sqlite3.connect(TEST_DB_PATH, timeout=1.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM streamed_messages WHERE message = ? AND author = ?",
                (test_message["message"], test_message["author"]),
            )
            row = cursor.fetchone()
            if row:
                test_message_id = row[0]
                # Delete the test message
                delete_message(test_message_id, TEST_DB_PATH)
            else:
                logger.warning("Test message not found; nothing to delete.")
    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve or delete test message: {e}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()