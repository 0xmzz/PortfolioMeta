import streamlit as st
import pandas as pd
from db.app_user_operations import (
    fetch_user_ids,
    fetch_addresses_for_user,
    get_all_wallet_addresses,
    save_raw_data_to_db,
    get_data_from_table,
    fetch_aggregated_data_for_user,
    fill_user_portfolio,
    get_tokens_for_user_without_spam,
    get_tokens_for_user,
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
    # Fetch aggregated chain data for user
    aggregated_data = fetch_aggregated_data_for_user(user_id)
    
    # Let the user set a threshold value for filtering
    threshold = st.slider("Set a USD threshold for displaying chains:", 0, 500, 10)
    st.session_state['threshold'] = threshold  # Store the threshold value in st.session_state
    show_zeros = st.checkbox("Show chains with $0 value", value=False)

    # Apply the filter
    if not show_zeros:
        aggregated_data = aggregated_data[(aggregated_data['aggregated_usd_value'] > threshold) | (aggregated_data['reported_usd_value'] > threshold)]

    # Calculate the aggregated USD Value and reported USD Value totals
    total_aggregated_usd_all_addresses = aggregated_data['aggregated_usd_value'].sum()
    total_reported_usd_all_addresses = aggregated_data['reported_usd_value'].sum()

    # Display totals and detailed breakdown
    st.write(f"**Total Aggregated USD Value (All Addresses)**: ${total_aggregated_usd_all_addresses:.2f}")
    st.write(f"**Total Reported USD Value (All Addresses)**: ${total_reported_usd_all_addresses:.2f}")
    st.write("**Aggregate Portfolio by Chain Compared with Reported Values**")
    st.table(aggregated_data)

def display_tokens_for_chain(user_id, user_data, chain, token_threshold):
    tokens_for_chain_raw = user_data[user_data['chain'] == chain]
    
    # Filter spam tokens and those with a value below the threshold
    tokens_for_chain = get_tokens_for_user_without_spam(user_id, tokens_for_chain_raw)
    tokens_for_chain = tokens_for_chain[tokens_for_chain['total_usd_value'] > token_threshold]
    
    st.write(f"**Tokens for Chain {chain}:**")
    st.table(tokens_for_chain[['name', 'total_usd_value', 'total_token_amount']])




def display_chain_and_token_data_for_address(user_id, user_data, address, usc_threshold, token_threshold):
    # Display the chosen address and its total USD value
    st.subheader(f"Data for Address: {address}")
    total_usd_value_for_address = user_data[user_data['wallet_address'] == address]['total_usd_value'].sum()
    st.write(f"**Total USD Value for Address {address}**: ${total_usd_value_for_address:.2f}")
    
    # Separate slider for token threshold
    token_threshold = st.slider("Set a USD threshold for displaying tokens:", 0, 500, 10)
    
    chains_for_address_df = user_data[user_data['wallet_address'] == address].groupby('chain').agg({'total_usd_value': 'sum'}).reset_index()
    chains_for_address_df = chains_for_address_df[chains_for_address_df['total_usd_value'] > usc_threshold]

    # User selects a chain from the filtered chains
    selected_chain = st.radio("Choose a Chain:", options=chains_for_address_df['chain'].tolist())
    
    # Display tokens associated with the selected chain
    tokens_for_chain_raw = user_data[(user_data['wallet_address'] == address) & (user_data['chain'] == selected_chain)]
    tokens_for_chain = get_tokens_for_user_without_spam(user_id, tokens_for_chain_raw)
    tokens_for_chain = tokens_for_chain[tokens_for_chain['total_usd_value'] > token_threshold]  # Apply the token threshold here
    
    st.write(f"**Tokens for Chain {selected_chain}**")
    st.table(tokens_for_chain[['name', 'total_usd_value', 'total_token_amount']])


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

    # Display amalgamated data
    with st.container():
        st.subheader("Amalgamated Data")
        display_chain_aggregated_data(selected_user_id)

    # Display token data for the selected address using radio widget
    addresses = user_data['wallet_address'].unique()
    selected_address = st.radio("Choose an Address:", options=addresses)
    
    # Retrieve the threshold value for chains from session state, with a default of 10 if not set
    usc_threshold = st.session_state.get('threshold', 10)

    # Now pass the usc_threshold value to the function call along with a placeholder for token_threshold
    # We'll get the actual token_threshold value from within the function
    display_chain_and_token_data_for_address(selected_user_id, user_data, selected_address, usc_threshold, None)



if __name__ == "__main__":
    handle_db_management()
