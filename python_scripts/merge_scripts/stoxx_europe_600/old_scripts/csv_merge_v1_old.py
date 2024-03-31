import pandas as pd
import os
from datetime import datetime
# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'


# Define a DataFrame to store the unique rows
unique_rows = pd.DataFrame(columns=['Emittententicker', 'Name', 'Sektor'])

# Loop through all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        # Construct the full path to the file
        file_path = os.path.join(directory, filename)
        # Read the CSV file, assuming the second line contains the column headers
        df = pd.read_csv(file_path, header=2, usecols=['Emittententicker', 'Name', 'Sektor'])
        # Drop duplicates within the same file
        df = df.drop_duplicates(subset=['Name'])
        # Append new unique rows to the unique_rows DataFrame based on 'Name'
        unique_rows = pd.concat([unique_rows, df]).drop_duplicates(subset=['Name'])

# List of words/phrases to check in the 'Name' column
words_to_remove = ['EURO STOXX', 'CASH', 'USD', "EUR", "COUPON", "GBP", "CHF", "DKK", "BON DE SOUSCRIPTION", "RIGHTS", "ACCEPTANCE LINE", "PAID RIGHT", "RIGHT", "FULLY PAID"]

# Build the regular expression pattern to check for the presence of the words/phrases
pattern = '|'.join(words_to_remove)

# Remove rows where the 'Name' column contains any of the specified words or phrases
unique_rows = unique_rows[~unique_rows['Name'].str.contains(pattern, case=False, na=False)]

# Remove rows with duplicate 'Emittententicker', keeping the first occurrence
unique_rows = unique_rows.drop_duplicates(subset=['Emittententicker'], keep='first')

# Save the unique rows to a new CSV file
unique_rows.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_overview_raw.csv', index=False)

