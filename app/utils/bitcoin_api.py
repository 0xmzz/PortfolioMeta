import requests

def get_bitcoin_address_info(address):
    url = f"https://chain.api.btc.com/v3/address/{address}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return None
