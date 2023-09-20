"""
Database Loader for Streamlit UI.
Allows for management of User IDs and database operations.
"""
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os
import datetime
from pathlib import Path
portfolio_meta_dir_path = Path(__file__).parent.parent.resolve()
# Convert the path to a string and add it to sys.path
if str(portfolio_meta_dir_path) not in sys.path:
    sys.path.append(str(portfolio_meta_dir_path))
from db.debank_api_operations import initialize_db
import streamlit as st
from address_management import manage_addresses
from db_management_buttons import handle_db_management



# Load .env variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# SQLAlchemy setup
engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

def main():
    """
    The main function that runs the Streamlit UI.
    """
    st.title('Database Loader')
    # Add a button to initialize the database
    if st.sidebar.button('Initialize Tables in Database'):
        initialize_db()
        with open('last_init_time.txt', 'w', encoding='utf-8') as f:
            f.write(str(datetime.datetime.now()))
        st.sidebar.success('Database initialized.')

    # Display the last time the database was initialized
    try:
        with open('last_init_time.txt', 'r', encoding='utf-8') as f:
            last_init_time = f.read()
        st.sidebar.write(f"Last initialized: {last_init_time}")
    except FileNotFoundError:
        st.sidebar.write("Database not initialized yet.")

    # Create a tab-like interface using st.radio
    tab_selection = st.radio("Choose a tab:", ["User ID Management", "Database Management"])
    if tab_selection == "User ID Management":
        manage_addresses()
    if tab_selection == "Database Management":
        handle_db_management()
if __name__ == '__main__':
    main()
