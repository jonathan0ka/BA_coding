import pandas as pd

#URL library
euro_stoxx_50 = "https://www.ishares.com/ch/privatkunden/de/produkte/251783/ishares-euro-stoxx-50-ucits-etf-de-fund/1495092304805.ajax?fileType=csv&fileName=DJSXE_holdings&dataType=fund&asOfDate={date}"
stoxx_europe_600 = "https://www.ishares.com/de/privatanleger/de/produkte/251931/ishares-stoxx-europe-600-ucits-etf-de-fund/1478358465952.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund&asOfDate={date}"
stoxx_europe_600_banks = "https://www.ishares.com/de/privatanleger/de/produkte/251934/ishares-stoxx-europe-600-banks-ucits-etf-de-fund/1478358465952.ajax?fileType=csv&fileName=EXV1_holdings&dataType=fund&asOfDate={date}"
stoxx_europe_600_health_care = "https://www.ishares.com/de/privatanleger/de/produkte/251946/ishares-stoxx-europe-600-health-care-ucits-etf-de-fund/1478358465952.ajax?fileType=csv&fileName=EXV4_holdings&dataType=fund&asOfDate={date}"

base_url = euro_stoxx_50

# Define your start and end dates in YYYY-MM-DD format
start_date = '2023-01-10' #updated
end_date = '2024-03-18'

# Generate a date range for every business day between the start and end dates
dates = pd.date_range(start=start_date, end=end_date, freq='D')

# Open a file to write
with open('euro_stoxx_50_links.csv', 'w') as file:
    # Write each link with the corresponding date to the file
    for date in dates:
        formatted_date = date.strftime('%Y%m%d')  # Format the date as needed
        url = base_url.replace('{date}', formatted_date)
        file.write(url + '\n')  # Write the URL to the file