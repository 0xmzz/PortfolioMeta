import streamlit as st
import pandas as pd
from db.app_user_operations import (
    fetch_user_ids,
    fetch_addresses_for_user,
    get_all_wallet_addresses,
    save_raw_data_to_db,
    get_data_from_table,
    get_user_data_with_chain_breakdown,
    fill_user_portfolio,
    get_tokens_for_user_without_spam,
    get_tokens_for_user,
    fetch_chain_data_for_user
)
from db.debank_db_setup import (
    initialize_db,
    drop_tables,
    initialize_history_tables,
    create_updated_at_trigger,
    create_backup_triggers,
    initialize_specific_tables,
    drop_specific_tables )
from services.debank_data_fetcher import fetch_total_balance, fetch_all_token_list
from datetime import datetime

# General Utilities
def display_status_message(message, status="info"):
    if status == "success":
        st.success(message)
    elif status == "error":
        st.error(message)
    elif status == "warning":
        st.warning(message)
    else:
        st.info(message)

# Database Management Operations
def get_database_status():
    user_ids = fetch_user_ids()
    return {user_id: fetch_addresses_for_user(user_id) for user_id in user_ids}

def display_database_status(user_data):
    st.write("## Database Status")
    for user_id, addresses in user_data.items():
        st.write(f"User ID: {user_id} - Total Addresses: {len(addresses)}")
        df = pd.DataFrame(addresses, columns=["Addresses"])
        st.write(df)

def fetch_and_load_data():
    wallet_addresses = get_all_wallet_addresses()
    
    for wallet_address in wallet_addresses:
        raw_balance_data = fetch_total_balance(wallet_address)
        raw_token_data = fetch_all_token_list(wallet_address)
        save_raw_data_to_db(wallet_address, raw_balance_data, raw_token_data)
    display_status_message("Data fetched and loaded into the database!", "success")

def initialize_database():
    initialize_db()
    initialize_history_tables()
    create_updated_at_trigger()
    create_backup_triggers()
    display_status_message("Database tables initialized!", "success")

def display_all_tables_data():
    tables = ["users", "Wallets", "Chains", "Tokens", "UserWallets", "WalletChainBalances", "WalletTokenBalances", "UserSpamFilters", "UserPortfolio"]
    for table in tables:
        data = get_data_from_table(table)
        df = pd.DataFrame(data)
        st.write(f"Data from {table}:")
        st.write(df)

def display_user_data_from_db(user_id):
    if user_id:
        user_data = fetch_addresses_for_user(user_id)
        df = pd.DataFrame(user_data, columns=["Addresses"])
        st.write(df)
        st.write(f"Total addresses for User ID {user_id}: {len(user_data)}")
    else:
        display_status_message("Please select a User ID.", "warning")


# Streamlit GUI
def handle_db_management():
    st.title("Database Management")
    
    # List of all the tables available for operations
    all_tables = ["users", "Wallets", "UserWallets", "Chains", "Tokens", 
                  "WalletChainBalances", "WalletTokenBalances", "UserSpamFilters", 
                  "UserPortfolio"]

    all_user_ids = fetch_user_ids()

    # Database Initialization and Debugging operations
    if st.sidebar.button('Initialize Database', key="init_db"):
        initialize_database()
        display_status_message("Database initialized!", "success")

    # Section for initializing specific tables
    tables_to_init = st.sidebar.multiselect('Select tables to initialize:', all_tables, key="init_select_tables")
    if st.sidebar.button('Initialize Specific Tables', key="init_specific"):
        initialize_specific_tables(tables_to_init)
        display_status_message(f"Initialized tables: {', '.join(tables_to_init)}", "success")

    # Section for dropping specific tables
    tables_to_drop = st.sidebar.multiselect('Select tables to drop:', all_tables, key="drop_select_tables")
    if st.sidebar.button('Drop Specific Tables', key="drop_specific"):
        drop_specific_tables(tables_to_drop)
        display_status_message(f"Dropped tables: {', '.join(tables_to_drop)}", "warning")

    if st.sidebar.button('Drop All Tables (Debugging)', key="drop_db"):
        drop_tables()
        display_status_message("All tables dropped for debugging!", "warning")

    if not all_user_ids:
        display_status_message("No User IDs found in the database.", "warning")
        return

    selected_user_id = st.sidebar.selectbox("Select User ID:", options=all_user_ids, key="user_id_selectbox_1")

    if st.sidebar.button('Check Database Status', key="check_db_status"):
        user_data = get_database_status()
        display_database_status(user_data)

    if st.sidebar.button('Fetch and Load Data', key="fetch_and_load"):
        fetch_and_load_data()

    if st.sidebar.button('Display All Tables', key="display_all_tables"):
        display_all_tables_data()

# Note: You'll need to implement the function `drop_specific_tables` in your db management code. 
# This function would drop only the tables that are passed as arguments.


def display_tokens_after_spam_filters(user_data, spam_tokens_set):
    filtered_tokens = user_data[~user_data['token_name'].isin(spam_tokens_set)]
    if not filtered_tokens.empty:
        st.write(filtered_tokens[['token_name', 'token_amount']])
    else:
        st.write("No tokens after applying spam filters.")


def mark_tokens_as_spam(tokens_for_address, address):
    unique_tokens = tokens_for_address['token_name'].unique().tolist()
    return st.multiselect(f"Select spam tokens for {address}", unique_tokens)

def display_chain_aggregated_data(user_id):
    # Fetch detailed user data
    user_data = get_user_data_with_chain_breakdown(user_id)
    
    # Aggregate data by chain from UserPortfolio
    aggregated_by_chain = user_data.groupby('chain_name').agg({'total_usd_value': 'sum'}).reset_index()

    # Fetch chain data for user
    chain_data = fetch_chain_data_for_user(user_id)
    
    # Merge the data from both tables
    merged_data = pd.merge(aggregated_by_chain, chain_data, on="chain_name", how="left")

    st.write("**Aggregate Portfolio by Chain Compared with Reported Values**")
    st.table(merged_data)

def handle_portfolio_management():
    st.title("Portfolio Management")
    
    # Fetching user details
    all_user_ids = fetch_user_ids()
    if not all_user_ids:
        st.warning("No User IDs found in the database.")
        return

    # Sidebar Elements
    st.sidebar.header("User Selection")
    selected_user_id = st.sidebar.selectbox("Select User ID:", options=all_user_ids, key="user_id_selectbox_2")
    
    # Now place the button execution
    if st.sidebar.button('Fill User Portfolio'):
        fill_user_portfolio(selected_user_id)
        st.success('User portfolio filled successfully!')

    # Fetch user token data
    user_data = get_tokens_for_user(selected_user_id).drop_duplicates()

 

    # Total USD value aggregated across unique wallet addresses
    total_usd_value_all_addresses = user_data['total_usd_value'].sum()
    st.write(f"**Total USD Value (All Addresses)**: ${total_usd_value_all_addresses:.2f}")

    # Containers for each address
    addresses = user_data['wallet_address'].unique()
    amalgamated_container = st.container()
    containers = {addr: st.container() for addr in addresses}

    for address, container in containers.items():
        with container.expander(f"Tokens for Address: {address}", expanded=True):
            tokens_for_address = user_data[user_data['wallet_address'] == address]
            
            # Display tokens after spam filters
            non_spam_tokens_df = get_tokens_for_user_without_spam(selected_user_id, tokens_for_address)
            st.write("**Tokens after Spam Filters:**")
            st.table(non_spam_tokens_df[['name', 'total_usd_value', 'total_token_amount']])

    with amalgamated_container.expander("Amalgamated Data", expanded=True) :
        display_chain_aggregated_data(selected_user_id)

if __name__ == "__main__":
    handle_db_management()
