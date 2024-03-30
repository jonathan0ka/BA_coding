###############################
# import pandas as pd
# import os
# from datetime import datetime

# # Specify the directory containing the CSV files
# directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'

# # Initialize a list to store DataFrames
# dfs = []

# # Loop through all the CSV files in the directory
# for filename in os.listdir(directory):
#     if filename.endswith('.csv'):
#         date_str = filename[-12:-4]  # Extract the date from the filename
#         file_date = datetime.strptime(date_str, '%Y%m%d')
#         file_path = os.path.join(directory, filename)
#         df = pd.read_csv(file_path, header=2, usecols=['Emittententicker', 'Name', 'Sektor', 'Standort', 'Börse', 'Gewichtung (%)'])
#         df['Datum'] = file_date
#         dfs.append(df)

# # Concatenate all data frames at once
# unique_rows = pd.concat(dfs)

# # Sort by 'Emittententicker' and 'Datum' (most recent first)
# unique_rows['Datum'] = pd.to_datetime(unique_rows['Datum'])
# unique_rows = unique_rows.sort_values(by=['Emittententicker', 'Datum'], ascending=[True, False])

# # Create a mapping from 'Emittententicker' to the most recent 'Name' for non "-" tickers
# ticker_to_name = unique_rows[unique_rows['Emittententicker'] != "-"].drop_duplicates(subset='Emittententicker').set_index('Emittententicker')['Name'].to_dict()

# # Apply the mapping to create the 'consistent_name' column, using 'Name' directly if the ticker is "-"
# unique_rows['consistent_name'] = unique_rows.apply(lambda row: row['Name'] if row['Emittententicker'] == "-" else ticker_to_name.get(row['Emittententicker'], row['Name']), axis=1)

# # Continue with the existing filtering
# words_to_remove = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE"]
# pattern = '|'.join(words_to_remove)
# unique_rows = unique_rows[~unique_rows['Name'].str.contains(pattern, case=False, na=False)]
# unique_rows = unique_rows[~unique_rows['Sektor'].str.contains("Cash und/oder Derivate", case=False, na=False)]
# unique_rows = unique_rows[unique_rows['Gewichtung (%)'] != '0,00']

# # Drop duplicates for 'Emittententicker' and 'Name' after filtering
# unique_rows = unique_rows.drop_duplicates(subset=['Emittententicker', 'Name'])

# # Save the unique rows to a new CSV file
# unique_rows.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_overview_raw.csv', index=False)


#################################################
import pandas as pd
import os
from datetime import datetime

# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/euro_stoxx_50_downloads'

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

# Sort by 'Emittententicker', 'Standort' and 'Datum' (most recent first)
unique_rows['Datum'] = pd.to_datetime(unique_rows['Datum'])
unique_rows = unique_rows.sort_values(by=['Emittententicker', 'Standort', 'Datum'], ascending=[True, True, False])

# Create a mapping for the most recent 'Name' based on 'Emittententicker' and 'Standort'
# For non "-" tickers
unique_rows_filtered = unique_rows[unique_rows['Emittententicker'] != "-"]
mapping = unique_rows_filtered.drop_duplicates(subset=['Emittententicker', 'Standort']).set_index(['Emittententicker', 'Standort'])['Name'].to_dict()

# Apply the mapping to create the 'consistent_name' column
def get_consistent_name(row):
    if row['Emittententicker'] == "-":
        return row['Name']
    return mapping.get((row['Emittententicker'], row['Standort']), row['Name'])

unique_rows['consistent_name'] = unique_rows.apply(get_consistent_name, axis=1)

# Continue with the existing filtering
words_to_remove_50_spec = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE", "PAID RIGHTS", "PAID", "FULLY PAID", "RIGHTS"]
#words_to_remove_600_spec = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE"]
words_to_remove = words_to_remove_50_spec


pattern = '|'.join(words_to_remove)
unique_rows = unique_rows[~unique_rows['Name'].str.contains(pattern, case=False, na=False)]
unique_rows = unique_rows[~unique_rows['Sektor'].str.contains("Cash und/oder Derivate", case=False, na=False)]
unique_rows = unique_rows[unique_rows['Gewichtung (%)'] != '0,00']

# Drop duplicates for 'Emittententicker', 'Name', and 'Standort' after filtering
unique_rows = unique_rows.drop_duplicates(subset=['Emittententicker', 'Name', 'Standort'])

# Save the unique rows to a new CSV file
unique_rows.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_50_overview_raw.csv', index=False)
