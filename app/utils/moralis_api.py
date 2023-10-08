import os
from moralis import sol_api

def get_solana_portfolio(address):
    api_key = os.getenv("MORALIS_API_KEY")
    params = {
        "network": "mainnet",
        "address": address
    }
    
    result = sol_api.account.get_portfolio(api_key=api_key, params=params)
    return result
