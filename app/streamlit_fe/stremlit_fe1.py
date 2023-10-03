"""
Database Loader for Streamlit UI.
Allows for management of User IDs and database operations.
"""

# Standard library imports
import os
from pathlib import Path
import sys

# Third-party imports
from dotenv import load_dotenv
import streamlit as st

# Define the path and append it to sys.path if necessary
portfolio_meta_dir_path = Path(__file__).parent.parent.resolve()
if str(portfolio_meta_dir_path) not in sys.path:
    sys.path.append(str(portfolio_meta_dir_path))

# Local imports
from address_management import manage_addresses
from db_management_buttons import handle_db_management, handle_portfolio_management
from spam_token_management import handle_spam_token_management
from db.app_user_operations import fetch_user_ids

# Load .env variables and ensure crucial variables are present
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL is not set in the environment.")


def display_status_message(message, status):
    """Utility function to display a status message."""
    # Assuming you have this function somewhere; if not, this needs implementation or removal.
    st.write(f"{status.upper()}: {message}")


def select_user_id():
    """
    Streamlit UI component for selecting a User ID.
    Returns the selected user ID or None if no User IDs are available.
    """
    all_user_ids = fetch_user_ids()
    if not all_user_ids:
        display_status_message("No User IDs found in the database.", "warning")
        return None

    st.sidebar.header("User Selection")
    selected_user_id = st.sidebar.selectbox("Select User ID:", options=all_user_ids, key="user_id_selectbox_3")
    return selected_user_id


def main():
    """
    The main function that runs the Streamlit UI.
    """
    st.title('Database Loader')

    # Tab selection interface
    tab_selection = st.radio(
        "Choose a tab:",
        ["User ID Management", "Database Management", "Portfolio Management", "Spam Token Management"]
    )

    selected_user_id = select_user_id()
    if not selected_user_id:
        return

    # Switch to appropriate tab function
    if tab_selection == "User ID Management":
        manage_addresses()
    elif tab_selection == "Database Management":
        handle_db_management()
    elif tab_selection == "Portfolio Management":
        handle_portfolio_management()
    elif tab_selection == "Spam Token Management":
        handle_spam_token_management(selected_user_id)


if __name__ == '__main__':
    main()
