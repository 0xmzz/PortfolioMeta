import json
import psycopg2
from decouple import config
import logging
logging.basicConfig(level=logging.INFO)


# Load environment variables from .env file
DATABASE_URL = config('DATABASE_URL').replace("postgresql+psycopg2://", "postgresql://")

# Utility functions for database operations

def execute_query(query, values=None):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
            conn.commit()

def execute_query_with_result(query, values=None):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, values)
            return cursor.fetchall()

# User ID Management functions

def fetch_user_ids():
    """Fetches all user IDs."""
    return [row[0] for row in execute_query_with_result("SELECT user_id FROM users")]

def create_user(user_id, addresses):
    """Creates a new user with given addresses."""
    addresses_json = json.dumps(addresses)
    execute_query('INSERT INTO users (user_id, addresses) VALUES (%s, %s)', (user_id, addresses_json))

def update_addresses_for_user(user_id, addresses):
    """Updates the addresses for a given user ID."""
    logging.info(f"Updating addresses for {user_id}: {addresses}")
    execute_query('''
INSERT INTO users (user_id, addresses) 
VALUES (%s, %s) 
ON CONFLICT (user_id) 
DO UPDATE SET addresses = EXCLUDED.addresses;
''', (user_id, json.dumps(addresses)))

def delete_user(user_id):
    """Deletes a user by ID."""
    execute_query('DELETE FROM users WHERE user_id = %s', (user_id,))

def add_address_to_user(user_id, address):
    """Adds a new address to a given user."""
    current_addresses = fetch_addresses_for_user(user_id)
    if address not in current_addresses:
        current_addresses.append(address)
        update_addresses_for_user(user_id, current_addresses)

def remove_address_from_user(user_id, address):
    """Removes an address from a given user. can have multiple addresses"""
    current_addresses = fetch_addresses_for_user(user_id)
    if not isinstance(address, list):  # Ensure that the address is a list
        address = [address]
    for addr in address:
        if addr in current_addresses:
            current_addresses.remove(addr)
    update_addresses_for_user(user_id, current_addresses)


def fetch_addresses_for_user(user_id):
    """Fetches addresses for a given user."""
    logging.info(f"Fetching addresses for user: {user_id}")
    addresses_json = execute_query_with_result('SELECT addresses FROM users WHERE user_id = %s', (user_id,))
    if addresses_json:
        logging.info(f"Retrieved address JSON for user {user_id}: {addresses_json[0][0]}")
        return json.loads(addresses_json[0][0])
    else:
        logging.warning(f"No addresses found for user: {user_id}")
        return []
