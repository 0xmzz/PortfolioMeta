import streamlit as st
from db.app_user_operations import (update_spam_tokens_in_db,
                                    get_previously_marked_spam_tokens,
                                    get_all_unique_tokens_for_user,
                                      get_non_spam_tokens_for_user,
                                        get_tokens_for_user)

def handle_spam_token_management(selected_user_id):
    st.title("Manage Spam Tokens")

    # Fetch tokens data for the user
    tokens_data = get_tokens_for_user(selected_user_id)

    # Get previously marked spam tokens
    previously_marked_spam_tokens = get_previously_marked_spam_tokens(selected_user_id)

    # Identify tokens that should be considered as spam by default
    default_spam_criteria = (~tokens_data['is_verified']) & (~tokens_data['is_core']) & (~tokens_data['is_wallet'])
    default_spam_tokens = set(tokens_data[default_spam_criteria]['name'])

    if st.button('Toggle All'):
        # If all tokens are currently marked as spam, set all of them to non-spam.
        if all(st.session_state.spam_selection.values()):
            for token_name in st.session_state.spam_selection:
                st.session_state.spam_selection[token_name] = False
        # Otherwise, mark all tokens as spam.
        else:
            for token_name in st.session_state.spam_selection:
                st.session_state.spam_selection[token_name] = True

    # Search functionality to filter tokens
    search_term = st.text_input("Search for a token:")
    if search_term:
        tokens_data = tokens_data[tokens_data['name'].str.contains(search_term, case=False)]
        
    if st.button("Save Spam Token Choices"):
        new_spam_tokens = {token_name for token_name, is_spam in st.session_state.spam_selection.items() if is_spam}
        update_spam_tokens_in_db(selected_user_id, new_spam_tokens)
        st.success("Spam token choices updated successfully!")

    if not hasattr(st.session_state, 'spam_selection'):
        st.session_state.spam_selection = {}

    for index, row in tokens_data.iterrows():
        display_text = f"{row['name']} (Amount: {row['total_token_amount']}, USD Value: {row['total_usd_value']})"
        if row['name'] not in st.session_state.spam_selection:
            st.session_state.spam_selection[row['name']] = row['name'] in previously_marked_spam_tokens
        is_spam = st.checkbox(display_text, key=f"{row['name']}_{index}", value=st.session_state.spam_selection[row['name']])
        st.session_state.spam_selection[row['name']] = is_spam





