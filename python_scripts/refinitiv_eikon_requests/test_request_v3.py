import eikon as ek

# Set your Eikon API key
ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Use a company name to search for its ticker
company_name = "Sika"  # Example company name
data, err = ek.get_data(instruments=[f"R:{company_name}"], fields=['RIC', 'Name', 'ExchangeName', 'CountryISO'])

if err is None:
    print(data)
else:
    print(f"Error: {err}")