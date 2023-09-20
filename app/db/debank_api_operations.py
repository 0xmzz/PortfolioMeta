import psycopg2
from decouple import config

DATABASE_URL = config('DATABASE_URL').replace("postgresql+psycopg2://", "postgresql://")

def execute_query(query, params=()):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()

def execute_query_with_result(query, params=()):
    with psycopg2.connect(DATABASE_URL) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

def initialize_db():
    # Wallets table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Wallets (
            address VARCHAR PRIMARY KEY,
            total_usd_value NUMERIC
        );
    ''')
    
    # Chains table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Chains (
            chain_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            logo_url VARCHAR,
            usd_value NUMERIC
        );
    ''')

    # WalletChainBalances table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS WalletChainBalances (
            address VARCHAR REFERENCES Wallets(address),
            chain_id VARCHAR REFERENCES Chains(chain_id),
            usd_value NUMERIC,
            PRIMARY KEY (address, chain_id)
        );
    ''')

    # Tokens table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Tokens (
            token_id VARCHAR PRIMARY KEY,
            chain_id VARCHAR REFERENCES Chains(chain_id),
            name VARCHAR,
            symbol VARCHAR,
            display_symbol VARCHAR,
            optimized_symbol VARCHAR,
            decimals INTEGER,
            logo_url VARCHAR,
            protocol_id VARCHAR,
            price REAL,
            price_24h_change REAL,
            is_verified BOOLEAN,
            is_core BOOLEAN,
            is_wallet BOOLEAN,
            time_at REAL
        );
    ''')
    # WalletTokenBalances table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS WalletTokenBalances (
            address VARCHAR REFERENCES Wallets(address),
            token_id VARCHAR REFERENCES Tokens(token_id),
            amount REAL,
            raw_amount REAL,
            raw_amount_hex_str VARCHAR,
            PRIMARY KEY (address, token_id)
        );
    ''')