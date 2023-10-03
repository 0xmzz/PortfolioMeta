import requests
from utils.debank_utils import headers, DEBANK_API_URL, to_decimal
from db.debank_db_setup import execute_query, execute_query_with_result

DEBUG = False # This can be sourced from environment variables, config files, or set as per your needs

def debug_print(msg):
    if DEBUG:
        print(msg)

def fetch_data_from_api(url, address=None):
    debug_print(f"Fetching data from {url}...")
    params = {}
    if address:
        sanitized_address = address.strip('{}')
        params['id'] = sanitized_address

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
    print("hereis raw balance date", raw_balance_data)
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
            INSERT INTO Chains (id, name, logo_url, wrapped_token_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET name = EXCLUDED.name, logo_url = EXCLUDED.logo_url, wrapped_token_id = EXCLUDED.wrapped_token_id;
        ''', (chain['id'], chain['name'], chain['logo_url'], chain['wrapped_token_id']))

        # 3. Insert or Update WalletChainBalances
        execute_query('''
            INSERT INTO WalletChainBalances (wallet_address, chain_id, usd_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, chain_id)
            DO UPDATE SET usd_value = EXCLUDED.usd_value;
        ''', (wallet_address, chain['id'], to_decimal(chain['usd_value'])))

    # 4. Insert or Update Tokens
    for token in raw_token_data:
        execute_query('''
            INSERT INTO Tokens (id, chain, name, symbol)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET chain = EXCLUDED.chain, name = EXCLUDED.name, symbol = EXCLUDED.symbol;
        ''', (token['id'], token['chain'], token['name'], token['symbol']))

        # 5. Insert or Update WalletTokenBalances
        execute_query('''
            INSERT INTO WalletTokenBalances (wallet_address, token_id, amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, token_id)
            DO UPDATE SET amount = EXCLUDED.amount;
        ''', (wallet_address, token['id'], to_decimal(token['amount'])))


def get_all_wallet_addresses():
    return [row[0] for row in execute_query_with_result("SELECT wallet_address FROM UserWallets;")]


if __name__ == "__main__":
    wallet_addresses = get_all_wallet_addresses()
    
    for wallet_address in wallet_addresses:
        raw_balance_data = fetch_total_balance(wallet_address)
        raw_token_data = fetch_all_token_list(wallet_address)
        save_raw_data_to_db(wallet_address, raw_balance_data, raw_token_data)
