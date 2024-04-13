import yfinance as yf

def search_ticker(company_name):
    # This is a basic implementation. The search can be refined with more advanced APIs or methods.
    # The function returns the first ticker symbol that matches the company name.
    search_results = yf.Ticker(company_name)
    return  search_results

# Example usage
company_name = "Microsoft"
ticker = search_ticker(company_name)
print(f"The ticker for {company_name} is: {ticker}")