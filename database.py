# database.py
import os
from pymongo import MongoClient
from pymongo.database import Database  # Import Database type for typing hints
from pymongo.mongo_client import MongoClient  # Import MongoClient type for typing hints
from pymongo.errors import ConnectionFailure
from fastapi import HTTPException, status
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "pupscribe")

client: MongoClient | None = None
database: Database | None = (
    None  # Use 'database' here to avoid conflict with the module name
)


def connect_db():
    """Establishes the MongoDB connection."""
    print("Attempting to connect to MongoDB during startup...")
    global client, database  # Declare intent to modify global variables
    try:
        # Ensure previous connections are closed/reset if connect is called multiple times
        if client:
            client.close()
        client = None
        database = None

        client = MongoClient(MONGO_URI)
        print(f"MongoClient created. Checking connection for DB '{DB_NAME}'...")

        # Access the database. This doesn't necessarily create it, but gives a Database object.
        temp_db = client[DB_NAME]

        # Ping the database to verify the connection is active and authentications are okay
        temp_db.command("ping")
        print("MongoDB ping successful.")

        # If ping succeeds, assign the Database object globally
        database = temp_db
        print(f"MongoDB connection to '{DB_NAME}' established successfully!")

    except ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}")
        # Ensure globals are None on failure
        client = None
        database = None
        # Re-raise the exception so the startup process fails visibly
        raise e
    except Exception as e:
        # Catch any other unexpected errors during the connection process
        print(f"An unexpected error occurred during MongoDB connection setup: {e}")
        # Ensure globals are None on failure
        client = None
        database = None
        # Re-raise the exception to signal startup failure
        raise e


def close_db():
    """Closes the MongoDB connection."""
    print("Closing MongoDB connection...")
    global client, database
    if client:
        client.close()
        print("MongoDB connection closed.")
    # Explicitly set globals to None after closing
    client = None
    database = None


def get_database():
    """FastAPI dependency to get the database object."""
    # This function is called by FastAPI for each request where the dependency is used.
    # It returns the global database object if it's not None.
    # If the startup failed, 'database' would be None, leading to an error when used.
    if database is None:
        # This case should ideally not be reached if startup was successful.
        # If it is, it means the dependency was called before startup finished
        # or startup failed silently (less likely with the improved connect_db).
        # Returning None or raising an error here helps diagnose issues.
        # Raising an HTTP exception is appropriate in a request context.
        print("Database dependency requested but database is not connected.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not connected",
        )

    return database
