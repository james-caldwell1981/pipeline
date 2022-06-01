from os import environ
from pathlib import Path
from dotenv import load_dotenv
import psycopg2


def load_credentials(cred_path):
    """
    Loads database crendtials from .env file into environment variables if cred_path is supplied.
    :param cred_path: String path to the .env file to load.
    :return: Success message as string
    """
    load_dotenv(Path(cred_path))

    return 'Successfully loaded environment variables.'


def connect_to_database(conn=None, cursor=True):
    """
    Generates a connection and cursor for the database. If conn is supplied and a valid psycopg2 connection,
    returns both the connection and a generated cursor. If cursor is set to False, only the connection gets
    returned. All return values are within a tuple for consistency.
    :param conn: A valid psycopg2 connection.
    :param cursor: A valid psycopg2 cursor.
    :return: Tuple of assets dictated by supplied arguments.
    """

    if conn is True:
        conn = psycopg2.connect(host=environ['HOST'],
                                database=environ['DB'],
                                user=environ['USER'],
                                password=environ['PASS'])
    elif conn is not None and conn.__name__ == psycopg2.connect().__name__:
        raise TypeError('Parameter conn not a valid psycopg2 connection.')
    elif conn is None:
        raise ConnectionAbortedError('Parameter conn must be psycopg2 connection to generate cursor.')
    if cursor is True:
        return (conn, conn.cursor())
    else:
        return tuple(conn)

