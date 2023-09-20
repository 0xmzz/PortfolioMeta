import requests
from utils.debank_utils import headers, DEBANK_API_URL
from db.operations import execute_query, execute_query_with_result
from utils.debank_utils import to_decimal

DEBUG = False # This can also be sourced from environment variables or config files if you wish

def debug_print(msg):
    if DEBUG:
        print(msg)

def fetch_data_from_api(url, address=None):
    debug_print(f"Fetching data from {url}...")
    params = {}
    if address:
        params['id'] = address
    try:
        response = requests.get(url, headers=headers, params=params)
        debug_print(f"Data fetched: {response.json()}")
        return response.json()
    except Exception as e:
        debug_print(f"Error fetching data: {e}")
        return []

def fetch_all_token_list(address):
    debug_print(f"Fetching all tokens for address: {address}...")
    url = f"{DEBANK_API_URL}/v1/user/all_token_list"
    return fetch_data_from_api(url, address=address)

def fetch_total_balance(address):
    url = f"{DEBANK_API_URL}/v1/user/total_balance"
    return fetch_data_from_api(url, address=address)

def save_raw_data_to_db(wallet_address, raw_balance_data, raw_token_data):
    # Insert or Update Wallet's data
    execute_query('''
        INSERT INTO Wallets (address, total_usd_value)
        VALUES (%s, %s)
        ON CONFLICT (address)
        DO UPDATE SET total_usd_value = EXCLUDED.total_usd_value;
    ''', (wallet_address, to_decimal(raw_balance_data['total_usd_value'])))

    # Iterating through chains
    for chain in raw_balance_data['chain_list']:
        # Insert or Update Chain's data
        execute_query('''
            INSERT INTO Chains (chain_id, name, logo_url, usd_value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chain_id)
            DO UPDATE SET name = EXCLUDED.name, logo_url = EXCLUDED.logo_url, usd_value = EXCLUDED.usd_value;
        ''', (chain['id'], chain['name'], chain['logo_url'], to_decimal(chain['usd_value'])))

        # Insert or Update WalletChainBalances
        execute_query('''
            INSERT INTO WalletChainBalances (address, chain_id, usd_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (address, chain_id)
            DO UPDATE SET usd_value = EXCLUDED.usd_value;
        ''', (wallet_address, chain['id'], to_decimal(chain['usd_value'])))

    # Iterating through tokens
    for token in raw_token_data:
        # Insert or Update Token's data
        execute_query('''
            INSERT INTO Tokens (token_id, chain_id, name, symbol, display_symbol, optimized_symbol, decimals, logo_url, protocol_id, price, price_24h_change, is_verified, is_core, is_wallet, time_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_id)
            DO UPDATE SET chain_id = EXCLUDED.chain_id, name = EXCLUDED.name, symbol = EXCLUDED.symbol, display_symbol = EXCLUDED.display_symbol, optimized_symbol = EXCLUDED.optimized_symbol, decimals = EXCLUDED.decimals, logo_url = EXCLUDED.logo_url, protocol_id = EXCLUDED.protocol_id, price = EXCLUDED.price, price_24h_change = EXCLUDED.price_24h_change, is_verified = EXCLUDED.is_verified, is_core = EXCLUDED.is_core, is_wallet = EXCLUDED.is_wallet, time_at = EXCLUDED.time_at;
        ''', (token['id'], token['chain'], token['name'], token['symbol'], token['display_symbol'], token['optimized_symbol'], token['decimals'], token['logo_url'], token['protocol_id'], to_decimal(token['price']), to_decimal(token['price_24h_change']), token['is_verified'], token['is_core'], token['is_wallet'], to_decimal(token['time_at'])))

        # Insert or Update WalletTokenBalances
        execute_query('''
            INSERT INTO WalletTokenBalances (address, token_id, amount, raw_amount, raw_amount_hex_str)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (address, token_id)
            DO UPDATE SET amount = EXCLUDED.amount, raw_amount = EXCLUDED.raw_amount, raw_amount_hex_str = EXCLUDED.raw_amount_hex_str;
        ''', (wallet_address, token['id'], to_decimal(token['amount']), to_decimal(token['raw_amount']), token['raw_amount_hex_str']))

