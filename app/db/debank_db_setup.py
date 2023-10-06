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
    

def drop_specific_tables(tables_to_drop):
    """
    Drop specific tables from the database.

    Args:
    - tables_to_drop (list): List of table names to be dropped.

    """
    valid_tables = [
        "Tokens_History", 
        "Chains_History", 
        "Wallets_History", 
        "Tokens", 
        "Chains", 
        "WalletChainBalances", 
        "WalletChainBalances_history", 
        "WalletTokenBalances", 
        "WalletTokenBalances_history", 
        "Wallets", 
        "UserPortfolio", 
        "UserPortfolio_history", 
        "UserSpamFilters", 
        "UserWallets", 
        "users",
        "NFTs",
        "NFTs_history"
        "Attributes",
        "Attributes_history",
        "bitcoin_addresses",
        "bitcoin_addresses_history"
    ]
    
    for table in tables_to_drop:
        if table in valid_tables:
            execute_query(f'DROP TABLE IF EXISTS {table} CASCADE;')
        else:
            print(f"Table {table} is not a valid table name and was not dropped.")

def drop_tables():
    execute_query('''
        DROP TABLE IF EXISTS Tokens_History CASCADE;
        DROP TABLE IF EXISTS Chains_History CASCADE;
        DROP TABLE IF EXISTS Wallets_History CASCADE;
        DROP TABLE IF EXISTS Tokens CASCADE;
        DROP TABLE IF EXISTS Chains CASCADE;
        DROP TABLE IF EXISTS WalletChainBalances CASCADE;
        DROP TABLE IF EXISTS WalletChainBalances_history CASCADE;         
        DROP TABLE IF EXISTS WalletTokenBalances CASCADE;
        DROP TABLE IF EXISTS WalletTokenBalances_history CASCADE;
        DROP TABLE IF EXISTS WALLETS CASCADE;
        DROP TABLE IF EXISTS Userportfolio CASCADE;
        DROP TABLE IF EXISTS Userportfolio_history CASCADE;
        DROP TABLE IF EXISTS UserSpamFilters CASCADE;
        DROP TABLE IF EXISTS UserWallets CASCADE;
        DROP TABLE IF EXISTS NFTs CASCADE;
        
    ''')

def initialize_db():
    # Create user table with user_id
    execute_query('''
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR PRIMARY KEY
        );
    ''')

    # Wallets table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Wallets (
            address VARCHAR PRIMARY KEY,
            total_usd_value NUMERIC,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
    ''')
    #this is a junction table for many to many relationship between users and wallets
    # UserWallets table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS UserWallets (
            user_id VARCHAR REFERENCES users(user_id),
            wallet_address VARCHAR REFERENCES Wallets(address),
            PRIMARY KEY (user_id, wallet_address)
        );
    ''')

    # Chains table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Chains (
            id VARCHAR,
            wallet_address VARCHAR REFERENCES Wallets(address),
            community_id NUMERIC,
            name VARCHAR,
            native_token_id VARCHAR,
            logo_url VARCHAR,
            wrapped_token_id VARCHAR,
            is_support_pre_exec BOOLEAN,
            usd_value NUMERIC,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY (id, wallet_address)
        );
    ''')


    # Tokens table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS Tokens (
            id VARCHAR(255) PRIMARY KEY,
            wallet_address VARCHAR REFERENCES Wallets(address),
            chain VARCHAR,
            name VARCHAR,
            symbol VARCHAR,
            display_symbol VARCHAR,
            optimized_symbol VARCHAR,
            decimals NUMERIC,
            logo_url VARCHAR(255),
            protocol_id VARCHAR,
            price NUMERIC,
            price_24h_change NUMERIC,
            is_verified BOOLEAN,
            is_core BOOLEAN,
            is_wallet BOOLEAN,
            time_at TIMESTAMP,
            amount NUMERIC,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
    ''')
    
    # NFT table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS NFTs (
        id VARCHAR PRIMARY KEY,
        contract_id VARCHAR,
        inner_id INT,
        chain VARCHAR,
        name VARCHAR,
        description TEXT,
        content_type VARCHAR,
        content VARCHAR,
        thumbnail_url VARCHAR,
        total_supply INT,
        detail_url VARCHAR,    
        collection_id VARCHAR,
        contract_name VARCHAR,
        is_erc721 BOOLEAN,
        is_erc1155 BOOLEAN,
        amount INT,
        usd_price DECIMAL(10,3),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
                              
    ''')

    # Attributes table initialization
    execute_query('''
        attribute_id SERIAL PRIMARY KEY,
        nft_id VARCHAR REFERENCES nfts(id),
        key VARCHAR,
        trait_type VARCHAR,
        value VARCHAR,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
    ''')

    execute_query('''
    CREATE TABLE IF NOT EXISTS bitcoin_addresses (
        address VARCHAR PRIMARY KEY,
        received BIGINT,
        sent BIGINT,
        balance BIGINT,
        tx_count INT,
        unconfirmed_tx_count INT,
        unconfirmed_received BIGINT,
        unconfirmed_sent BIGINT,
        unspent_tx_count INT,
        first_tx VARCHAR,
        last_tx VARCHAR
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
''')
    execute_query('''
    CREATE TABLE IF NOT EXISTS Tokens_sol (
        associated_token_address VARCHAR PRIMARY KEY,
        mint VARCHAR,
        amount_raw VARCHAR,
        amount DECIMAL,
        decimals VARCHAR,
        name VARCHAR,
        symbol VARCHAR,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
''')
    execute_query('''
    CREATE TABLE IF NOT EXISTS NFTs_sol (
        associated_token_address VARCHAR PRIMARY KEY,
        mint VARCHAR,
        amount_raw VARCHAR,
        amount DECIMAL,
        decimals VARCHAR,
        name VARCHAR,
        symbol VARCHAR,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
''')
    execute_query('''
    CREATE TABLE IF NOT EXISTS Native_Balance_sol (
        user_id SERIAL PRIMARY KEY,  -- Assuming each user has a unique ID
        lamports VARCHAR,
        solana DECIMAL, 
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
''')
                  
       # WalletChainBalances table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS WalletChainBalances (
            wallet_address VARCHAR(255) REFERENCES Wallets(address),
            chain_id VARCHAR(255),
            usd_value NUMERIC,
            PRIMARY KEY (wallet_address, chain_id)
        );
    ''')

    # WalletTokenBalances table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS WalletTokenBalances (
            wallet_address VARCHAR REFERENCES Wallets(address),
            token_id VARCHAR REFERENCES Tokens(id),
            amount NUMERIC,
            PRIMARY KEY (wallet_address, token_id)
        );
    ''')


    execute_query('''
        CREATE TABLE IF NOT EXISTS UserSpamFilters (
            user_id VARCHAR PRIMARY KEY REFERENCES users(user_id),
            spam_tokens VARCHAR[]
        );
    ''')


    # Setting up triggers after table creations

    # UserPortfolio table initialization
    execute_query('''
        CREATE TABLE IF NOT EXISTS UserPortfolio (
            user_id VARCHAR REFERENCES users(user_id),
            token_id VARCHAR REFERENCES Tokens(id),
            wallet_address VARCHAR REFERENCES Wallets(address),
            chain VARCHAR,
            name VARCHAR(255),  
            total_token_amount NUMERIC(30, 15),
            total_usd_value NUMERIC(30, 15),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY (user_id, token_id, wallet_address)
        );
    ''')

    # Setting up triggers after table creations
    create_updated_at_trigger()

def initialize_history_tables():
    history_tables = ["Wallets", "Chains", "Tokens", "NFTs", "UserPortfolio", "WalletChainBalances", "WalletTokenBalances", "Attributes","bitcoin_addresses"]
    for table in history_tables:
        execute_query(f'''
            CREATE TABLE IF NOT EXISTS {table}_History AS
                TABLE {table} WITH NO DATA;
            ALTER TABLE {table}_History ADD COLUMN backup_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL;
        ''')

def initialize_specific_tables(tables_to_init):
    table_initialization = {
        "users": '''
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR PRIMARY KEY
            );
        ''',
        "Wallets": '''
            CREATE TABLE IF NOT EXISTS Wallets (
                address VARCHAR PRIMARY KEY,
                total_usd_value NUMERIC,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            );
        ''',
        "UserWallets": '''
            CREATE TABLE IF NOT EXISTS UserWallets (
                user_id VARCHAR REFERENCES users(user_id),
                wallet_address VARCHAR REFERENCES Wallets(address),
                PRIMARY KEY (user_id, wallet_address)
            );
        ''',
        "Chains": '''
            CREATE TABLE IF NOT EXISTS Chains (
                id VARCHAR,
                wallet_address VARCHAR REFERENCES Wallets(address),
                community_id NUMERIC,
                name VARCHAR,
                native_token_id VARCHAR,
                logo_url VARCHAR,
                wrapped_token_id VARCHAR,
                is_support_pre_exec BOOLEAN,
                usd_value NUMERIC,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                PRIMARY KEY (id, wallet_address)
            );
        ''',
        "Tokens": '''
            CREATE TABLE IF NOT EXISTS Tokens (
                id VARCHAR(255) PRIMARY KEY,
                wallet_address VARCHAR REFERENCES Wallets(address),
                chain VARCHAR,
                name VARCHAR,
                symbol VARCHAR,
                display_symbol VARCHAR,
                optimized_symbol VARCHAR,
                decimals NUMERIC,
                logo_url VARCHAR(255),
                protocol_id VARCHAR,
                price NUMERIC,
                price_24h_change NUMERIC,
                is_verified BOOLEAN,
                is_core BOOLEAN,
                is_wallet BOOLEAN,
                time_at TIMESTAMP,
                amount NUMERIC,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
            );
                ''',
        "NFTs": '''
                CREATE TABLE IF NOT EXISTS NFTs (
                id VARCHAR PRIMARY KEY,
                contract_id VARCHAR,
                inner_id INT,
                chain VARCHAR,
                name VARCHAR,
                description TEXT,
                content_type VARCHAR,
                content VARCHAR,
                thumbnail_url VARCHAR,
                total_supply INT,
                detail_url VARCHAR,    
                collection_id VARCHAR,
                contract_name VARCHAR,
                is_erc721 BOOLEAN,
                is_erc1155 BOOLEAN,
                amount INT,
                usd_price DECIMAL(10,3),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );
                                    
                ''' ,

        "Attributes": '''
                attribute_id SERIAL PRIMARY KEY,
                nft_id VARCHAR REFERENCES nfts(id),
                key VARCHAR,
                trait_type VARCHAR,
                value VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );

                ''' ,
        "bitcoin_addresses": '''
                address VARCHAR PRIMARY KEY,
                received BIGINT,
                sent BIGINT,
                balance BIGINT,
                tx_count INT,
                unconfirmed_tx_count INT,
                unconfirmed_received BIGINT,
                unconfirmed_sent BIGINT,
                unspent_tx_count INT,
                first_tx VARCHAR,
                last_tx VARCHAR
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );
                ''',
        "Tokens_sol": '''
                associated_token_address VARCHAR PRIMARY KEY,
                mint VARCHAR,
                amount_raw VARCHAR,
                amount DECIMAL,
                decimals VARCHAR,
                name VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );
                ''',
        "NFTs_sol": '''
                associated_token_address VARCHAR PRIMARY KEY,
                mint VARCHAR,
                amount_raw VARCHAR,
                amount DECIMAL,
                decimals VARCHAR,
                name VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );
                ''',
        "Native_Balance_sol": '''
                user_id SERIAL PRIMARY KEY,
                lamports VARCHAR,
                solana DECIMAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
                );
                ''',
                    
        "WalletChainBalances": '''
                    CREATE TABLE IF NOT EXISTS WalletChainBalances (
                        wallet_address VARCHAR(255) REFERENCES Wallets(address),
                        chain_id VARCHAR(255),
                        usd_value NUMERIC,
                        PRIMARY KEY (wallet_address, chain_id)
                    );
                ''',
        "WalletTokenBalances": '''
                    CREATE TABLE IF NOT EXISTS WalletTokenBalances (
                        wallet_address VARCHAR REFERENCES Wallets(address),
                        token_id VARCHAR REFERENCES Tokens(id),
                        amount NUMERIC,
                        PRIMARY KEY (wallet_address, token_id)
                    );
                ''',
        "UserSpamFilters": '''
                    CREATE TABLE IF NOT EXISTS UserSpamFilters (
                        user_id VARCHAR PRIMARY KEY REFERENCES users(user_id),
                        spam_tokens VARCHAR[]
                    );
                ''',
        "UserPortfolio": '''
                    CREATE TABLE IF NOT EXISTS UserPortfolio (
                        user_id VARCHAR REFERENCES users(user_id),
                        token_id VARCHAR REFERENCES Tokens(id),
                        wallet_address VARCHAR REFERENCES Wallets(address),
                        chain VARCHAR,
                        name VARCHAR(255),
                        total_token_amount NUMERIC(30, 15),
                        total_usd_value NUMERIC(30, 15),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                        PRIMARY KEY (user_id, token_id, wallet_address)
                    );
                '''
    }

    for table in tables_to_init:
        if table in table_initialization:
            execute_query(table_initialization[table])


def create_updated_at_trigger():
    # Create or replace the function
    execute_query('''
        CREATE OR REPLACE FUNCTION update_modified_column()
        RETURNS TRIGGER AS $$
        BEGIN
           NEW.updated_at = now();
           RETURN NEW;
        END;
        $$ language 'plpgsql';
    ''')

    # Create the triggers for the necessary tables
    tables_with_update_time = ["Wallets", "Chains", "Tokens", "WalletChainBalances", "WalletTokenBalances", "UserPortfolio", "NFTs", "Attributes", "bitcoin_addresses", "Tokens_sol", "NFTs_sol", "Native_Balance_sol"]
    for table in tables_with_update_time:
        # Check if the trigger already exists
        result = execute_query_with_result(f'''
            SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'update_{table.lower()}_modtime'
            );
        ''')
        if not result[0][0]:  # If trigger doesn't exist, create it
            execute_query(f'''
                CREATE TRIGGER update_{table.lower()}_modtime
                BEFORE UPDATE ON {table}
                FOR EACH ROW
                EXECUTE FUNCTION update_modified_column();
            ''')

def create_backup_triggers():
    backup_tables = ["Wallets", "Chains", "Tokens", "WalletChainBalances", "WalletTokenBalances", "UserPortfolio", "NFTs", "Attributes", "bitcoin_addresses", "Tokens_sol", "NFTs_sol", "Native_Balance_sol"]
    for table in backup_tables:
        # Check if history table exists
        result = execute_query_with_result(f'''
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables 
                WHERE table_name = '{table.lower()}_history'
            );
        ''')
        if result[0][0]:  # Only create the trigger if history table exists
            execute_query(f'''
                CREATE OR REPLACE FUNCTION backup_{table.lower()}_trigger()
                RETURNS TRIGGER AS $$
                BEGIN
                    -- Insert the OLD state into the history table with the current timestamp
                    INSERT INTO {table}_History SELECT OLD.*, now();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';

                CREATE TRIGGER backup_{table.lower()}_before_update_trigger
                BEFORE UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION backup_{table.lower()}_trigger();
            ''')

def create_spam_tokens_fk_trigger():
    execute_query('''
        CREATE OR REPLACE FUNCTION check_spam_tokens_fk()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM Tokens WHERE id = ANY(NEW.spam_tokens)) THEN
                RAISE EXCEPTION 'Foreign key violation: some spam tokens are not valid tokens.';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ''')

    execute_query('''
        DROP TRIGGER IF EXISTS enforce_spam_tokens_fk ON UserSpamFilters;
        CREATE TRIGGER enforce_spam_tokens_fk
        BEFORE INSERT OR UPDATE ON UserSpamFilters
        FOR EACH ROW
        EXECUTE FUNCTION check_spam_tokens_fk();
    ''')

if __name__ == "__main__":
    initialize_db()
    initialize_history_tables()
    create_backup_triggers()
    create_updated_at_trigger()
    print("Database initialized successfully!")
