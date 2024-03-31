import pandas as pd
import os
from datetime import datetime

# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/ishares_data/ishares_raw_data/stoxx_europe_600_downloads'

# Initialize a list to store DataFrames
dfs = []

# Loop through all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        date_str = filename[-12:-4]  # Extract the date from the filename
        file_date = datetime.strptime(date_str, '%Y%m%d')
        file_path = os.path.join(directory, filename)
        df = pd.read_csv(file_path, header=2, usecols=['Emittententicker', 'Name', 'Sektor', 'Standort', 'Börse', 'Gewichtung (%)'])
        df['Datum'] = file_date
        dfs.append(df)

# Concatenate all data frames at once
unique_rows = pd.concat(dfs)

# Continue with the existing filtering
words_to_remove_600_spec = ['EURO STOXX']
#words_to_remove_600_spec = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE"]
words_to_remove = words_to_remove_600_spec

pattern = '|'.join(words_to_remove)
unique_rows = unique_rows[~unique_rows['Name'].str.contains(pattern, case=False, na=False)]
unique_rows = unique_rows[~unique_rows['Sektor'].str.contains("Cash und/oder Derivate", case=False, na=False)]

# Drop duplicates for 'Emittententicker', 'Name', and 'Standort' after filtering
unique_rows = unique_rows.drop_duplicates(subset=['Emittententicker', 'Name', 'Standort'])

##############################################################################################
# cleaning
##############################################################################################
# Rename columns
unique_rows.columns = ["ticker", "name", "sector", "weight", "region", "stock_exchange", "date"]

# Replace empty strings with NaN and then drop rows with NaN in the 'name' column
unique_rows['weight'] = pd.to_numeric(unique_rows['weight'], errors='coerce')
unique_rows['name'].replace("", pd.NA, inplace=True)
unique_rows.dropna(subset=['name'], inplace=True)

############################################################################

unique_rows = unique_rows.sort_values(by="name")

# Save the unique rows to a new CSV file
unique_rows.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/ishares_data/ishares_merged_data/stoxx_europe_600/stoxx_europe_600_overview_raw.csv', index=False)
