"""
This is a collection of functions for to connect to a PostgreSQL database.
"""

from os import environ
from pathlib import Path
from dotenv import load_dotenv
import psycopg2


def load_credentials(cred_path: str) -> str:
    """
    Loads database credentials from .env file at cred_path into environment variables.

    Examples:
    >>> load_credentials("testing/.env_test")
    'Successfully loaded environment variables.'

    Args:
    cred_path (str): String path to the .env file to load.

    Returns:
        str: Success message
    """
    load_dotenv(Path(cred_path))

    return 'Successfully loaded environment variables.'


def connect_to_database(conn: bool = False, cursor: bool = True) -> tuple:
    """
    Generates a connection and cursor for the database.

    Examples:
    >>> connect_to_database(conn=True, cursor=True)
    (conn, conn.cursor())
    >>> connect_to_database(conn=True, cursor=False)
    (conn)
    >>> connect_to_database(conn=False, cursor=True)
    (conn.cursor())

    Args:
    conn (bool): Boolean indicating need to return connection object.
    cursor (bool): A valid psycopg2 cursor.

    Returns:
        tuple: Tuple of requested assets with the connection at index 0 when multiple assets are requested.
    """

    if conn is True:
        conn = psycopg2.connect(host=environ['HOST'],
                                database=environ['DB'],
                                user=environ['USER'],
                                password=environ['PASS'])
    if cursor is True:
        return tuple(conn, conn.cursor())
    else:
        return tuple(conn)

