import streamlit as st
from db.app_user_operations import (update_spam_tokens_in_db,
                                    get_previously_marked_spam_tokens,
                                    get_all_unique_tokens_for_user,
                                      get_non_spam_tokens_for_user,
                                        get_tokens_for_user)

def get_block_explorer_url(chain, token_id):
    # Dictionary mapping chains to block explorer URLs
    chain_to_url = {
        'arb': 'https://arbiscan.io/address/',
        'astar': 'https://astar.subscan.io/account/',
        'aurora': 'https://explorer.aurora.dev/address/',
        'avax': 'https://snowtrace.io/address/',
        'base': 'https://basescan.org/address/',
        'boba': 'https://bobascan.com/',
        'brise': 'https://brisescan.com/',
        'bsc': 'https://bscscan.com/',
        'btt': 'https://bttcscan.com/',
        'canto': 'https://www.mintscan.io/canto/',
        'celo': 'https://explorer.celo.org/mainnet/',
        'cfx': 'https://www.confluxscan.io/',
        'ckb': 'https://v1.gwscan.com/',
        'core': 'https://scan.coredao.org/',
        'cro': 'https://cronoscan.com/',
        'dfk': 'https://avascan.info/blockchain/dfk/home',
        'doge': 'https://explorer.dogechain.dog/',
        'eos': 'https://bloks.io/key/',
        'era': 'https://explorer.zksync.io/address/',
        'etc': 'https://etcblockexplorer.com/search?q=',
        'eth': 'https://etherscan.io/address/',
        'evmos': 'https://www.mintscan.io/evmos/address/',
        'flr': 'https://flare-explorer.flare.network/address/',
        'fsn': 'https://fsnscan.com/address/',
        'ftm': 'https://ftmscan.com/address/',
        'fuse': 'https://explorer.fuse.io/address/',
        'heco': 'https://www.hecoinfo.com/en-us/address/',
        'hmy': 'https://explorer.harmony.one/address/',
        'iotx': 'https://iotexscan.io/address/',
        'kava': 'https://kavascan.com/address/',
        'klay': 'https://scope.klaytn.com/account/',
        'linea': 'https://lineascan.build/address/',
        'loot': 'https://explorer.lootchain.com/address/',
        'manta': 'https://pacific-explorer.manta.network/address/',
        'matic': 'https://polygonscan.com/address/',
        'mnt': 'https://explorer.mantle.xyz/address/',
        'mobm': 'https://moonscan.io/address/',
        'movr': 'https://moonriver.moonscan.io/address/',
        'mtr': 'https://scan.meter.io/address/',
        'nova': 'https://nova-explorer.arbitrum.io/address/',
        'oas': 'https://explorer.oasys.games/address/',
        'okt': 'https://www.oklink.com/oktc/address/',
        'op': 'https://optimistic.etherscan.io/address/',
        'opbnb': 'https://opbnbscan.com/address/',
        'palm': 'https://explorer.palm.io/address/',
        'pze': 'https://zkevm.polygonscan.com/address/',
        'rsk': 'https://explorer.rsk.co/address/',
        'xdai': 'https://gnosisscan.io/address/'
    }
    
    # Getting the URL prefix from the dictionary
    url_prefix = chain_to_url.get(chain, None)
    
    # If the chain is not supported
    if url_prefix is None:
        return None  # or return 'Unsupported chain'
    
    # Constructing the full URL
    full_url = f'{url_prefix}{token_id}'
    
    return full_url

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
        # Get the block explorer URL for the current token's chain
        token_url = get_block_explorer_url(row['chain'], row['token_id'])
        
        # Create clickable link to block explorer
        link = f"[{row['token_id']}]({token_url})"
        
        # Include the clickable link in the display text
        display_text = f"{row['name']} (Amount: {row['total_token_amount']}, USD Value: {row['total_usd_value']}, token address: {link}, chain: {row['chain']})"
        
        if row['name'] not in st.session_state.spam_selection:
            st.session_state.spam_selection[row['name']] = row['name'] in previously_marked_spam_tokens
        is_spam = st.checkbox(f"Mark {row['name']} as spam", key=f"{row['name']}_{index}", value=st.session_state.spam_selection[row['name']])
        st.session_state.spam_selection[row['name']] = is_spam
        st.markdown(f"**Token Info:**\n{display_text}", unsafe_allow_html=True)  # Use markdown with unsafe_allow_html=True
