import json
import psycopg2
from decouple import config
from utils.debank_utils import to_decimal
import logging
import pandas as pd
from datetime import datetime
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
# User ID Management functions

def fetch_user_ids():
    """Fetches all user IDs."""
    return [row[0] for row in execute_query_with_result("SELECT user_id FROM users")]

def create_user(user_id):
    """Creates a new user."""
    execute_query('INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING;', (user_id,))
    
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
    """Deletes a user by ID and their associated wallets."""
    # Deleting wallet associations first
    execute_query('DELETE FROM UserWallets WHERE user_id = %s', (user_id,))
    execute_query('DELETE FROM users WHERE user_id = %s', (user_id,))

def add_address_to_user(user_id, address):
    """Adds a new address to a given user."""
    # First ensure address exists in Wallets table.
    execute_query('INSERT INTO Wallets (address) VALUES (%s) ON CONFLICT (address) DO NOTHING;', (address,))
    # Then associate the user with the address in UserWallets
    execute_query('INSERT INTO UserWallets (user_id, wallet_address) VALUES (%s, %s) ON CONFLICT (user_id, wallet_address) DO NOTHING;', (user_id, address))

def remove_address_from_user(user_id, address):
    """Removes an address association from a given user."""
    execute_query('DELETE FROM UserWallets WHERE user_id = %s AND wallet_address = %s', (user_id, address))


def fetch_addresses_for_user(user_id):
    try:
        return [row[0] for row in execute_query_with_result('SELECT wallet_address FROM UserWallets WHERE user_id = %s', (user_id,))]
    except Exception as e:
        print(f"Error fetching addresses for user: {e}")
        return []
    

def save_raw_data_to_db(wallet_address, raw_balance_data, raw_token_data):
   
    
   
    # 1. Insert or Update Wallet's data
    execute_query('''
        INSERT INTO Wallets (address, total_usd_value)
        VALUES (%s, %s)
        ON CONFLICT (address)
        DO UPDATE SET total_usd_value = EXCLUDED.total_usd_value;
    ''', (wallet_address, to_decimal(raw_balance_data['total_usd_value'])))

    # 2. Insert or Update Chains

    for chain in raw_balance_data['chain_list']:
        
        execute_query('''
            INSERT INTO Chains (id, wallet_address, name, logo_url, wrapped_token_id, community_id, native_token_id, is_support_pre_exec, usd_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id, wallet_address)
            DO UPDATE SET 
                name = EXCLUDED.name, 
                logo_url = EXCLUDED.logo_url, 
                wrapped_token_id = EXCLUDED.wrapped_token_id,
                community_id = EXCLUDED.community_id,
                native_token_id = EXCLUDED.native_token_id,
                is_support_pre_exec = EXCLUDED.is_support_pre_exec,
                usd_value = EXCLUDED.usd_value;
        ''', (chain['id'], wallet_address, chain['name'], chain['logo_url'], chain['wrapped_token_id'], chain.get('community_id'), chain.get('native_token_id'), chain.get('is_support_pre_exec'), to_decimal(chain['usd_value'])))

        # 3. Insert or Update WalletChainBalances

        execute_query('''
            INSERT INTO WalletChainBalances (wallet_address, chain_id, usd_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, chain_id)
            DO UPDATE SET usd_value = EXCLUDED.usd_value;
        ''', (wallet_address, chain['id'], to_decimal(chain['usd_value'])))
        

    # 4. Insert or Update Tokens

   
    for token in raw_token_data:
        # Convert UNIX timestamp to datetime
        time_at_value = None
        if 'time_at' in token and isinstance(token.get('time_at'), (int, float)):
            try:
                time_at_value = datetime.utcfromtimestamp(token.get('time_at')).strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass

        # Prepare the values to insert
        values_to_insert = (
            token['id'], wallet_address, token['chain'], token['name'], 
            token['symbol'], token.get('display_symbol'), token.get('optimized_symbol'), 
            token.get('decimals'), token.get('logo_url'), token.get('protocol_id'), 
            to_decimal(token['price']), to_decimal(token.get('price_24h_change')), 
            token.get('is_verified'), token.get('is_core'), token.get('is_wallet'), 
            time_at_value, to_decimal(token.get('amount'))
    )

        # Execute the insertion query
        execute_query('''
            INSERT INTO Tokens (id, wallet_address, chain, name, symbol, display_symbol, optimized_symbol, decimals, logo_url, protocol_id, price, price_24h_change, is_verified, is_core, is_wallet, time_at, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET 
                chain = EXCLUDED.chain, 
                name = EXCLUDED.name, 
                symbol = EXCLUDED.symbol,
                display_symbol = EXCLUDED.display_symbol,
                optimized_symbol = EXCLUDED.optimized_symbol,
                decimals = EXCLUDED.decimals,
                logo_url = EXCLUDED.logo_url,
                protocol_id = EXCLUDED.protocol_id,
                price = EXCLUDED.price,
                price_24h_change = EXCLUDED.price_24h_change,
                is_verified = EXCLUDED.is_verified,
                is_core = EXCLUDED.is_core,
                is_wallet = EXCLUDED.is_wallet,
                time_at = EXCLUDED.time_at,
                amount = EXCLUDED.amount
        ''', values_to_insert)
        
     
        # 5. Insert or Update WalletTokenBalances
        execute_query('''
            INSERT INTO WalletTokenBalances (wallet_address, token_id, amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, token_id)
            DO UPDATE SET amount = EXCLUDED.amount;
        ''', (wallet_address, token['id'], to_decimal(token['amount'])))


def get_user_data_with_chain_breakdown(user_id):
    """
    Fetches a detailed breakdown of user data, including chain and token information.
    
    Returns:
        df (pd.DataFrame): A DataFrame containing the user data breakdown, or None if an error occurs.
    """
    query = """
    SELECT 
        up.user_id,
        up.wallet_address,
        up.total_usd_value,
        up.chain as chain_name,
        t.name as token_name,
        up.total_token_amount as token_amount
    FROM 
        UserPortfolio up
    JOIN Tokens t ON up.token_id = t.id
    WHERE up.user_id = %s;
    """

    df = None

    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:   # Add this line to define the cursor within the context
                cursor.execute(query, (user_id,))
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    except Exception as e:
        print(f"Error fetching user data with chain breakdown: {e}")

    return df

def fetch_chain_data_for_user(user_id):
    """
    Fetches chain data associated with a specific user.
    """
    query = """
    SELECT 
        c.name AS chain_name,
        c.usd_value AS reported_usd_value
    FROM 
        Chains c
    JOIN 
        Wallets w ON c.wallet_address = w.address
    JOIN 
        UserWallets uw ON w.address = uw.wallet_address
    WHERE uw.user_id = %s;
    """

    df = None
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (user_id,))
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    except Exception as e:
        print(f"Error fetching chain data: {e}")

    # Debugging line to see the data being fetched
    print("Chain Data for User:", df)

    return df

def fill_user_portfolio(user_id):
    query = """
    INSERT INTO UserPortfolio (user_id, token_id, name, total_token_amount, total_usd_value, wallet_address, chain)
    SELECT 
        uw.user_id,
        wtb.token_id,
        t.name,
        SUM(wtb.amount),
        SUM(wtb.amount * t.price),
        uw.wallet_address,
        t.chain  -- Added chain information
    FROM UserWallets uw
    JOIN WalletTokenBalances wtb ON uw.wallet_address = wtb.wallet_address
    JOIN Tokens t ON wtb.token_id = t.id
    WHERE uw.user_id = %s
    GROUP BY uw.user_id, wtb.token_id, t.name, uw.wallet_address, t.chain  -- Group by chain as well
    ON CONFLICT (user_id, token_id, wallet_address)  -- Consider whether 'chain' should be part of the conflict target
    DO UPDATE SET 
        name = EXCLUDED.name, 
        total_token_amount = EXCLUDED.total_token_amount, 
        total_usd_value = EXCLUDED.total_usd_value,
        chain = EXCLUDED.chain;  -- Updating chain information
"""

    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            cur = conn.cursor()
            cur.execute(query, (user_id,))
            conn.commit()
    except Exception as e:
        print(f"Error filling user portfolio: {e}")




def get_data_from_table(table_name, user_id=None):
    if user_id:
        query = f"SELECT * FROM {table_name} WHERE user_id = %s;"
        return execute_query_with_result(query, (user_id,))
    else:
        query = f"SELECT * FROM {table_name};"
        return execute_query_with_result(query)

def get_tokens_for_user(user_id):
    """Fetches tokens for a specific user from the UserPortfolio table."""
    query = f"""
    SELECT 
        up.user_id, up.token_id, up.wallet_address, up.name, up.total_token_amount, up.total_usd_value, up.timestamp, up.updated_at,
        t.is_verified, t.is_core, t.is_wallet 
    FROM UserPortfolio up
    JOIN Tokens t ON up.token_id = t.id
    WHERE up.user_id = %s;
    """
    rows = execute_query_with_result(query, (user_id,))
    
    columns = ["user_id", "token_id", "wallet_address", "name", "total_token_amount", "total_usd_value", "timestamp", "updated_at", 
               "is_verified", "is_core", "is_wallet"]
    df = pd.DataFrame(rows, columns=columns)
    
    return df






def get_tokens_for_user_without_spam(user_id, tokens_for_address_df):
    """Fetches tokens for a user excluding the spam tokens for a given address, returning a DataFrame."""
    
    # Fetch spam tokens for the user
    spam_tokens_row = execute_query_with_result('SELECT spam_tokens FROM UserSpamFilters WHERE user_id = %s', (user_id,))
    
    if spam_tokens_row:
        spam_tokens_list = spam_tokens_row[0][0]
        non_spam_tokens_df = tokens_for_address_df[~tokens_for_address_df['name'].isin(spam_tokens_list)]
    else:
        non_spam_tokens_df = tokens_for_address_df.copy()
    
    return non_spam_tokens_df


def get_non_spam_tokens_for_user(user_id):
    all_tokens_df = get_tokens_for_user(user_id)
    all_tokens_set = set(all_tokens_df['name'].tolist())

    spam_tokens_set = get_previously_marked_spam_tokens(user_id)
    
    non_spam_tokens = all_tokens_set.difference(spam_tokens_set)
    return list(non_spam_tokens)


def get_previously_marked_spam_tokens(user_id):
    """Fetches spam tokens for a user."""
    
    spam_tokens_row = execute_query_with_result('SELECT spam_tokens FROM UserSpamFilters WHERE user_id = %s', (user_id,))
    if spam_tokens_row:
        return set(spam_tokens_row[0][0])
    else:
        return set()
def update_spam_tokens_in_db(user_id, spam_tokens_set):
    """
    Update the spam tokens in the database for a specific user.
    
    Parameters:
        user_id (str): The ID of the user.
        spam_tokens_set (set): Set of tokens marked as spam by the user.
    """
    
    # Convert the spam tokens set to a list
    spam_tokens_list = list(spam_tokens_set)
    
    # Check if a record already exists for the user
    existing_record = execute_query_with_result('SELECT 1 FROM UserSpamFilters WHERE user_id = %s', (user_id,))
    
    if existing_record:
        # If a record exists, update the spam tokens for the user
        query = 'UPDATE UserSpamFilters SET spam_tokens = %s WHERE user_id = %s'
        execute_query(query, (spam_tokens_list, user_id))
    else:
        # If no record exists, insert a new one
        query = 'INSERT INTO UserSpamFilters (user_id, spam_tokens) VALUES (%s, %s)'
        execute_query(query, (user_id, spam_tokens_list))
def get_all_unique_tokens_for_user(user_id):

    """Fetches all unique tokens for a specific user across all addresses."""
    query = f"SELECT DISTINCT name FROM UserPortfolio WHERE user_id = %s;"
    rows = execute_query_with_result(query, (user_id,))
    return [row[0] for row in rows]



def get_all_wallet_addresses():
    return [row[0] for row in execute_query_with_result("SELECT wallet_address FROM UserWallets;")]

