import streamlit as st
from db.app_user_operations import (add_address_to_user, 
                                   fetch_user_ids, 
                                   fetch_addresses_for_user, 
                                   remove_address_from_user)


def is_valid_ethereum_address(address):
    # Basic validation for Ethereum address (length and starting with 0x)
    return len(address) == 42 and address.startswith("0x")


def manage_addresses():
    """
    Manage Ethereum addresses for a user ID.
    """
    st.markdown("# Ethereum Address Manager")
    
    all_user_ids = fetch_user_ids()
    all_user_ids.append("Create New")

    selected_user_id = st.selectbox("Select User ID", all_user_ids, key="user_id_selectbox")
    
    # Handle new user ID creation
    if selected_user_id == "Create New":
        new_user_id = st.text_input("Enter a new User ID:", key="new_user_id_input")
        if new_user_id:
            selected_user_id = new_user_id

    st.markdown("## Existing Addresses")

    # Display existing addresses with delete button
    saved_addresses = fetch_addresses_for_user(selected_user_id)
    deleted_addresses = []
    for address in saved_addresses:
        col1, col2 = st.columns(2)
        col1.write(address)
        if col2.button("Delete", key=f"delete_{address}"):
            
            deleted_addresses.append(address)
            

    for addr in deleted_addresses:
        try:
            
            saved_addresses.remove(addr)
            remove_address_from_user(selected_user_id, addr)
            print(saved_addresses)
            st.write(f"Address {addr} removed for {selected_user_id}!")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"An error occurred: {e}")

    st.markdown("## Add New Address")
        
    # Add a new address
    new_address = st.text_input("Add a new address:", key="new_address_input")
    if st.button("Save Address", key="save_address") and new_address:
        if is_valid_ethereum_address(new_address):
            try:
                add_address_to_user(selected_user_id, [new_address])
                st.write(f"Address {new_address} saved for {selected_user_id}!")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"An error occurred: {e}")
        else:
            st.warning("Invalid Ethereum address. Please check and try again.")
