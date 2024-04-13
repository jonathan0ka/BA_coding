######### translations
# from googletrans import Translator

# def translate_country_name(german_country_name):
#     translator = Translator()
#     translation = translator.translate(german_country_name, src='de', dest='en')
#     return translation.text

##########

# Your Alpha Vantage API key
api_key = '9Z6QJ5YZLJL28EGA'


import requests

def search_tickers(company_name, api_key):
    base_url = "https://www.alphavantage.co/query"
    params = {
        "function": "SYMBOL_SEARCH",
        "keywords": company_name,
        "apikey": api_key,
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        matches = data.get('bestMatches', [])
        
        # Extract the top 3 matches, if available
        top_matches = matches[:3]
        
        if top_matches:
            # Return a list of dictionaries containing the symbol and name of each match
            return [{'symbol': match['1. symbol'], 'name': match['2. name']} for match in top_matches]
        else:
            return ["No matches found."]
    else:
        return ["Failed to fetch data from Alpha Vantage."]

# Usage example:
matches = search_tickers("HERMES INTERNATIONAL", api_key)
print(matches)
