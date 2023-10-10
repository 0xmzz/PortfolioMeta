import psycopg2
import os
from dotenv import load_dotenv
from decimal import Decimal, InvalidOperation
from decouple import config
from datetime import datetime
import logging
timestamp = datetime.utcfromtimestamp(1633341217.0).strftime('%Y-%m-%d %H:%M:%S')
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
DATABASE_URL = config('DATABASE_URL').replace("postgresql+psycopg2://", "postgresql://")

# Utility functions for database operations
def execute_query(query, values=None):
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, values)
                conn.commit()
    except Exception as e:
        print(f"Error executing query: {e}")


def execute_query_with_result(query, values=None):
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, values)
                return cursor.fetchall()
    except Exception as e:
        print(f"Error executing query: {e}")
        return []

