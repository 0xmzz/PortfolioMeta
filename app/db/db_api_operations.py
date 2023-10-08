import psycopg2
from app_user_operations import execute_query, execute_query_with_result ,to_decimal
from datetime import datetime



def insert_update_evm_wallet(wallet_address, raw_balance_data):
    query = '''
        INSERT INTO Wallets (address, total_usd_value)
        VALUES (%s, %s)
        ON CONFLICT (address)
        DO UPDATE SET total_usd_value = EXCLUDED.total_usd_value;
    '''
    values = (wallet_address, to_decimal(raw_balance_data['total_usd_value']))
    execute_query(query, values)

def insert_update_evm_chains(wallet_address, raw_balance_data_debank):
    # Loop through each chain in the raw_balance_data_debank['chain_list']
    for chain in raw_balance_data_debank['chain_list']:
        # Construct the SQL query for inserting/updating chain data
        query = '''
            INSERT INTO Chains (
                id, wallet_address, name, logo_url, wrapped_token_id,
                community_id, native_token_id, is_support_pre_exec, usd_value
            )
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
        '''
        # Prepare the values to be inserted/updated
        values = (
            chain['id'], wallet_address, chain['name'], chain['logo_url'],
            chain['wrapped_token_id'], chain.get('community_id'), chain.get('native_token_id'),
            chain.get('is_support_pre_exec'), to_decimal(chain['usd_value'])
        )
        # Execute the query
        execute_query(query, values)

def insert_update_wallet_chain_balances(wallet_address, raw_balance_data_debank):
    for chain in raw_balance_data_debank['chain_list']:
        query = '''
            INSERT INTO WalletChainBalances (wallet_address, chain_id, usd_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, chain_id)
            DO UPDATE SET usd_value = EXCLUDED.usd_value;
        '''
        values = (wallet_address, chain['id'], to_decimal(chain['usd_value']))
        execute_query(query, values)


def insert_update_evm_tokens(wallet_address, raw_token_data_debank):
    # Loop through each token in raw_token_data_debank
    for token in raw_token_data_debank:
        # Convert UNIX timestamp to datetime
        time_at_value = None
        if 'time_at' in token and isinstance(token.get('time_at'), (int, float)):
            try:
                time_at_value = datetime.utcfromtimestamp(token.get('time_at')).strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # Construct the SQL query for inserting/updating token data
        query = '''
            INSERT INTO Tokens (
                id, wallet_address, chain, name, symbol, 
                display_symbol, optimized_symbol, decimals, 
                logo_url, protocol_id, price, price_24h_change, 
                is_verified, is_core, is_wallet, time_at, amount
            )
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
                amount = EXCLUDED.amount;
        '''
        # Prepare the values to be inserted/updated
        values = (
            token['id'], wallet_address, token['chain'], token['name'],
            token['symbol'], token.get('display_symbol'), token.get('optimized_symbol'),
            token.get('decimals'), token.get('logo_url'), token.get('protocol_id'),
            to_decimal(token['price']), to_decimal(token.get('price_24h_change')),
            token.get('is_verified'), token.get('is_core'), token.get('is_wallet'),
            time_at_value, to_decimal(token.get('amount'))
        )
        # Execute the query
        execute_query(query, values)

def insert_update_wallet_token_balances(wallet_address, raw_token_data_debank):
    for token in raw_token_data_debank:
        query = '''
            INSERT INTO WalletTokenBalances (wallet_address, token_id, amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (wallet_address, token_id)
            DO UPDATE SET amount = EXCLUDED.amount;
        '''
        values = (wallet_address, token['id'], to_decimal(token['amount']))
        execute_query(query, values)



def insert_update_evm_nfts(wallet_address, raw_evm_nft_data):
    for nft in raw_evm_nft_data:
        # Insert or update NFTs table
        nft_query = '''
            INSERT INTO NFTs (
                id, wallet_address, contract_id, inner_id, chain,
                name, description, content_type, content, thumbnail_url,
                total_supply, detail_url, collection_id, contract_name,
                is_erc721, is_erc1155, amount, usd_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET
                DO UPDATE SET
                wallet_address = EXCLUDED.wallet_address,
                contract_id = EXCLUDED.contract_id,
                inner_id = EXCLUDED.inner_id,
                chain = EXCLUDED.chain,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                content_type = EXCLUDED.content_type,
                content = EXCLUDED.content,
                thumbnail_url = EXCLUDED.thumbnail_url,
                total_supply = EXCLUDED.total_supply,
                detail_url = EXCLUDED.detail_url,
                collection_id = EXCLUDED.collection_id,
                contract_name = EXCLUDED.contract_name,
                is_erc721 = EXCLUDED.is_erc721,
                is_erc1155 = EXCLUDED.is_erc1155,
                amount = EXCLUDED.amount,
                usd_price = EXCLUDED.usd_price;
        '''
        nft_values = (
            nft['id'], wallet_address, nft['contract_id'], nft['inner_id'],
            nft['chain'], nft['name'], nft['description'], nft['content_type'],
            nft['content'], nft['thumbnail_url'], nft['total_supply'],
            nft['detail_url'], nft['collection_id'], nft['contract_name'],
            nft['is_erc721'], nft['is_erc1155'], nft['amount'], to_decimal(nft['usd_price'])
        )
        execute_query(nft_query, nft_values)
        
        # Insert or update Attributes table
        attributes = nft.get('attributes', [])
        for attribute in attributes:
            attribute_query = '''
                INSERT INTO Attributes (
                    wallet_address, nft_id, key, trait_type, value
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (attribute_id)
                DO UPDATE SET
                    nft_id = EXCLUDED.nft_id,
                    key = EXCLUDED.key,
                    trait_type = EXCLUDED.trait_type,
                    value = EXCLUDED.value;

            '''
            attribute_values = (
                wallet_address, nft['id'], attribute['key'],
                attribute['trait_type'], attribute['value']
            )
            execute_query(attribute_query, attribute_values)


def insert_update_solana_wallet(wallet_address, raw_solana_data):
    # Handling Native_Balance_sol table
    native_balance = raw_solana_data.get('native_balance', {})
    execute_query('''
        INSERT INTO Native_Balance_sol (
            wallet_address, lamports, solana
        ) VALUES (%s, %s, %s)
        ON CONFLICT (wallet_address)
        DO UPDATE SET
            lamports = EXCLUDED.lamports,
            solana = EXCLUDED.solana;
    ''', (
        wallet_address, native_balance.get('lamports'), to_decimal(native_balance.get('solana'))
    ))
    
    # Handling Tokens_sol table
    for token in raw_solana_data.get('tokens', []):
        execute_query('''
            INSERT INTO Tokens_sol (
                wallet_address, associated_token_address, mint, amount_raw,
                amount, decimals, name, symbol
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (wallet_address, associated_token_address)
            DO UPDATE SET
                mint = EXCLUDED.mint,
                amount_raw = EXCLUDED.amount_raw,
                amount = EXCLUDED.amount,
                decimals = EXCLUDED.decimals,
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol;
        ''', (
            wallet_address, token['associated_token_address'], token['mint'],
            token['amount_raw'], to_decimal(token['amount']), token['decimals'],
            token['name'], token['symbol']
        ))

    # Handling NFTs_sol table
    for nft in raw_solana_data.get('nfts', []):
        execute_query('''
            INSERT INTO NFTs_sol (
                wallet_address, associated_token_address, mint, amount_raw,
                amount, decimals, name, symbol
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (wallet_address, associated_token_address)
            DO UPDATE SET
                mint = EXCLUDED.mint,
                amount_raw = EXCLUDED.amount_raw,
                amount = EXCLUDED.amount,
                decimals = EXCLUDED.decimals,
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol;
        ''', (
            wallet_address, nft['associated_token_address'], nft['mint'],
            nft['amount_raw'], to_decimal(nft['amount']), nft['decimals'],
            nft['name'], nft['symbol']
        ))


def insert_update_bitcoin_wallet(wallet_address, raw_bitcoin_data):
    # Construct the query for Bitcoin Wallet
    btc_query = '''
        INSERT INTO Native_Balance_btc (
            wallet_address, user_id, received, sent, balance, tx_count,
            unconfirmed_tx_count, unconfirmed_received, unconfirmed_sent,
            unspent_tx_count, first_tx, last_tx
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        ON CONFLICT (wallet_address)
        DO UPDATE SET
            received = EXCLUDED.received,
            sent = EXCLUDED.sent,
            balance = EXCLUDED.balance,
            tx_count = EXCLUDED.tx_count,
            unconfirmed_tx_count = EXCLUDED.unconfirmed_tx_count,
            unconfirmed_received = EXCLUDED.unconfirmed_received,
            unconfirmed_sent = EXCLUDED.unconfirmed_sent,
            unspent_tx_count = EXCLUDED.unspent_tx_count,
            first_tx = EXCLUDED.first_tx,
            last_tx = EXCLUDED.last_tx;
    '''
    btc_values = (
        wallet_address, raw_bitcoin_data['user_id'],
        raw_bitcoin_data['balance'], to_decimal(raw_bitcoin_data['btc'])
    )
    execute_query(btc_query, btc_values)

