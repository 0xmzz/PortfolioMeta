import streamlit as st
import pandas as pd
import sys
sys.path.append("/absolute/path/to/project_root")
from db.app_user_operations import (
    fetch_user_ids,
    create_user,
    add_address_to_user,
    fetch_addresses_for_user,
    delete_user,
    remove_address_from_user
)

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
        st.write(f"User ID: {user_id}")
        for address in addresses:
            st.write(f"--- Address: {address}")

# Streamlit GUI
def handle_db_management():
    st.title("Database Management")

    all_user_ids = fetch_user_ids()
    selected_user_id = st.sidebar.selectbox("Select User ID:", options=all_user_ids, key="user_id_selectbox")

    if st.sidebar.button('Check Database Status', key="check_db_status"):
        user_data = get_database_status()
        display_database_status(user_data)
    if st.sidebar.button('Load User Data from Database', key="load_user_data_from_db"):
        display_user_data_from_db(selected_user_id)

def display_user_data_from_db(user_id):
    if user_id:
        user_data = fetch_addresses_for_user(user_id)
        process_user_data(user_id, user_data)
    else:
        display_status_message("Please select a User ID.", "warning")

def process_user_data(user_id, user_data):
    st.write(f"User ID: {user_id}")
    for address in user_data:
        st.write(f"--- Address: {address}")

def manage_user_ids(user_ids):
    selected_user_id = st.sidebar.selectbox("Select User ID", user_ids, key="user_id_selectbox")
    manage_addresses_for_user(selected_user_id)
    create_new_user()
    delete_selected_user(selected_user_id)

def manage_addresses_for_user(user_id):
    st.sidebar.subheader(f"Manage Addresses for {user_id}")
    st.sidebar.write("Addresses:")
    st.sidebar.write("\n".join(fetch_addresses_for_user(user_id)))

    new_address = st.sidebar.text_input("Add New Address", key="new_address_input")
    if st.sidebar.button("Add Address", key="add_new_address_button") and new_address:
        add_address_to_user(user_id, new_address)
        st.sidebar.success(f"Address {new_address} added for User ID {user_id}")

    address_to_delete = st.sidebar.selectbox("Select an address to delete", fetch_addresses_for_user(user_id), key="address_delete_selectbox")
    if st.sidebar.button("Delete Address", key="delete_address_button") and address_to_delete:
        remove_address_from_user(user_id, address_to_delete)
        st.sidebar.success(f"Address {address_to_delete} deleted for User ID {user_id}")

def create_new_user():
    st.sidebar.subheader("Create New User ID")
    user_id_input = st.sidebar.text_input("Enter new User ID alias", "")
    addresses_textarea = st.sidebar.text_area("Enter addresses (one per line):", "")
    addresses = [address.strip() for address in addresses_textarea.split("\n") if address.strip()]

    if st.sidebar.button("Save New User ID and Addresses", key="create_user_id_button") and user_id_input and addresses:
        create_user(user_id_input, addresses)
        st.sidebar.success(f"User ID {user_id_input} created with addresses.")

def delete_selected_user(user_id):
    if st.sidebar.button("Delete Selected User ID", key="delete_user_id_button"):
        delete_user(user_id)
        st.sidebar.success(f"User ID {user_id} deleted.")

def handle_user_management():
    user_ids = fetch_user_ids()
    if user_ids:
        manage_user_ids(user_ids)
    else:
        display_status_message("No User IDs found in database.", "warning")

if __name__ == "__main__":
    handle_db_management()
    handle_user_management()