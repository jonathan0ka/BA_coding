import eikon as ek

# Set your Eikon API key
ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# # Use a company name to search for its ticker
# company_name = "Sika"  # Example company name
# data, err = ek.get_data(instruments=[f"R:{company_name}"], fields=['RIC', 'Name', 'ExchangeName', 'CountryISO'])

# if err is None:
#     print(data)
# else:
#     print(f"Error: {err}")


########################## ticker without stock exchange code
# Specify just the ticker symbol without the exchange code
ticker_symbol = 'ABBN'  # Apple's ticker symbol, without specifying the exchange

# Define the fields you want to retrieve
fields = ['TR.CompanyName', 'TR.PriceClose', 'TR.Volume']

try:
    # Fetch the data
    data, err = ek.get_data(instruments=[ticker_symbol], fields=fields)

    if err:
        print(f"Error: {err}")
    else:
        print("Data retrieved successfully:")
        print(data)
except Exception as e:
    print(f"An error occurred: {e}")