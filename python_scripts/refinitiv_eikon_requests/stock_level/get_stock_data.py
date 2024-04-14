import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Define the date range
start_date = '2013-01-01'
end_date = '2024-01-01'

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\constituents_stoxx_europe_600.csv')  # Make sure to provide the correct path

# Extract the RICs into a list
ric_list = ric_df['Constituent RIC'].tolist()
print(ric_list)

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

# Generate the last business day of each month within the date range
business_days = pd.date_range(start=start_date, end=end_date, freq="D")