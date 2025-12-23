# Import Python's built-in SQLite module
# This gives us access to the SQLite database engine
import sqlite3


# Path to the SQLite database file
# Since this is a relative path, SQLite will create the file
# in the current working directory if it doesn't exist
DB_PATH = "data.db"


def get_connection():
    """
    Opens and returns a connection to the SQLite database.
    
    This function:
    - Connects to the data.db file
    - Allows usage across threads (needed for FastAPI)
    - Returns rows as dictionary-like objects
    """
    
    # Open a connection to the database file
    # check_same_thread=False allows FastAPI to use this connection
    # in different request-handling threads
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    
    # Configure rows to behave like dictionaries instead of tuples
    # This allows accessing columns by name (row["symbol"])
    conn.row_factory = sqlite3.Row
    
    return conn


def init_db():
    """
    Initializes the database schema.
    
    This function:
    - Creates the market_data table if it does not exist
    - Is safe to call multiple times
    - Does NOT delete or reset data
    """
    
    # Open a connection to the database
    conn = get_connection()
    
    # Create a cursor to execute SQL commands
    cursor = conn.cursor()

    # Execute SQL to create the table if it doesn't already exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            strike REAL NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)

    # Commit the transaction so the table definition is saved
    conn.commit()
    
    # Close the connection to free resources
    conn.close()