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
words_to_remove_50_spec = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE", "DIVIDEND"]
#words_to_remove_600_spec = ['EURO STOXX', "COUPON", "BON DE SOUSCRIPTION", "ACCEPTANCE LINE"]
words_to_remove = words_to_remove_50_spec


pattern = '|'.join(words_to_remove)
unique_rows = unique_rows[~unique_rows['Name'].str.contains(pattern, case=False, na=False)]
unique_rows = unique_rows[~unique_rows['Sektor'].str.contains("Cash und/oder Derivate", case=False, na=False)]

############################################################################ 
# add mean weight for each name
############################################################################

# Calculate the mean weight for each unique 'consistent_name' and merge it back to the original dataframe
mean_weight = unique_rows.groupby('Name')['Gewichtung (%)'].mean().reset_index(name='mean_weight')
unique_rows = pd.merge(unique_rows, mean_weight, on='Name', how='left')

# Drop duplicates for 'Emittententicker', 'Name', and 'Standort' after filtering
unique_rows = unique_rows.drop_duplicates(subset=['Emittententicker', 'Name', 'Standort'])

##############################################################################################
# cleaning
##############################################################################################
# Rename columns
unique_rows.columns = ["ticker", "name", "sector", "weight", "region", "stock_exchange", "date", "consistent_name", "mean_weight"]

# Replace empty strings with NaN and then drop rows with NaN in the 'name' column
unique_rows['weight'] = pd.to_numeric(unique_rows['weight'], errors='coerce')
unique_rows['name'].replace("", pd.NA, inplace=True)
unique_rows.dropna(subset=['name'], inplace=True)

# Assuming name_mapping is a dictionary in Python that mirrors the R list structure
# For example:
name_mapping = {
    "ANHEUSER-BUSCH INBEV SA": ["ANHEUSER BUSCH INBEV SA"],
    "ARCELORMITTAL NPV": ["ARCELORMITTAL SA"],
    "ASML HOLDING NV": ["ASML HOLDING N.V."],
    "BANCO SANTANDER SA": ["BANCO SANTANDER RFD", "BANCO SANTANDER RIGHTS SA"],
    "BAYER AG": ["BAYER RIGHTS AG", "BAYER AG"],
    "BBVA": ["BBVA RIGHTS", "BBVA APR 2015", "BBVA"],
    "BANCO SANTANDER SA": ["Banco Santander Rights", "BANCO SA DER RIGHTS SA", "BANCO SANTANDER", "BANCO SANTAN DER RTS SA", "BANCO SANTANDER RFD", "BANCO SANTANDER RIGHTS SA", "BANCO SANTANDER SA"],
    "BANCO BILBAO VIZCAYA ARGENTA": ["BANCO BILBAO VIZCAYA ARGENTARIA RI", "BANCO BILBAO VIZCAYA ARGENTA"],
    "DAIMLER TRUCK AG": ["DAIMLER TRUCK INTERIM LINE"],
    "ESSILORLUXOTTICA SA": ["ESSILOR INTERNATIONAL COMPAGNIE GE", "ESSILOR INTERNATIONAL SA"],
    "MERCEDES-BENZ GROUP N AG": ["DAIMLER AG"],
    "DEUTSCHE BANK AG": ["DEUTSCHE BANK RIGHTS AG"],
    "DEUTSCHE BOERSE AG": ["DEUTSCHE BOERSE AG-NEW"],
    "DEUTSCHE TELEKOM N AG": ["DEUTSCHE TELEKOM AG"],
    "IBERDROLA SA": ["IBERDROLA S.A."],
    "INTESA SANPAOLO": ["INTESA SANPAOLO SPA"],
    "KONINKLIJKE AHOLD DELHAIZE NV": ["KONINKLIJKE AHOLD NV"],
    "LVMH": ["Morgan Stanley Certificate on LVMH"],
    "REPSOL SA": ["REPSOL RIGHTS SA", "REPSOL", "REPSOL SA", "REPSOL SA RIGHTS SA"],
    "SIEMENS ENERGY N AG": ["SIEMENS ENERGY AG"],
    "TELEFONICA SA": ["TELEFONICA DER", "TELEFONICA RIGHTS SA", "TELEFONICA SA - INTERIM SHARES", "TELEFONICA SA RTS"],
    "TOTALENERGIES": ["TOTAL", "TOTAL SA"],
    "UNIVERSAL MUSIC GROUP NV": ["UNIVERSAL MUSIC GROUP"],
    "VOLKSWAGEN AG": ["VOLKSWAGEN NON-VOTING PREF AG", "VOLKSWAGEN AG (PFD NON-VTG)"],
    "VONOVIA SE": ["VONOVIA RIGHTS"] 
}

# Function to update consistent_name based on the mapping
def update_consistent_name(name, consistent_name, mapping):
    for key, values in mapping.items():
        if name in values:
            return key
    return consistent_name

# Apply the function to update the 'consistent_name' column
unique_rows['consistent_name'] = unique_rows.apply(lambda row: update_consistent_name(row['name'], row['consistent_name'], name_mapping), axis=1)

############################################################################

# Save the unique rows to a new CSV file
unique_rows.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_50_overview_raw.csv', index=False)

#############################################################################################
# merge
#############################################################################################

import pandas as pd
import os

# Load the overview DataFrame
relevant_df = unique_rows

# Ensure the 'date' column is in datetime format
relevant_df['date'] = pd.to_datetime(relevant_df['date'])

# Create a mapping from name to consistent_name
name_to_consistent_name = dict(zip(relevant_df['name'], relevant_df['consistent_name']))

# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/euro_stoxx_50_downloads'

# Initialize an empty list to store the DataFrames
data_frames = []

# Loop through all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        current_df = pd.read_csv(file_path, header=2)
        
        # Extract the date from the filename and add it as a new column
        date_str = filename[-12:-4]
        current_df['date'] = pd.to_datetime(date_str, format='%Y%m%d')
        
        # Filter the DataFrame based on the names in the overview file
        current_df = current_df[current_df['Name'].isin(name_to_consistent_name.keys())]
        
        # Add the 'consistent_name' and 'consistent_ticker' columns based on the 'Name' column mapping
        current_df['consistent_name'] = current_df['Name'].apply(lambda x: name_to_consistent_name[x])
        #current_df['consistent_ticker'] = current_df['consistent_name'].apply(lambda x: consistent_ticker_dict[x])
        
        # Append the filtered and updated DataFrame to the list
        data_frames.append(current_df)

# Concatenate all data frames
collected_data = pd.concat(data_frames, ignore_index=True)
collected_data.columns = ["ticker", "name", "sector", "asset_class","market_cap", "weight", "nominal_value", "nominal", "price", "country", "stock_exchange","currency","date", "consistent_name"]


##########################################################
# merging weight of rows with the same conistent_name
##########################################################
import numpy as np
import os

# Assuming collected_data is already defined as per your previous script

# Define a function to combine weights within groups
def combine_weights(group):
    if len(group) > 1:
        max_weight_idx = group['weight'].idxmax()
        min_weight_idx = group['weight'].idxmin()
        group.loc[max_weight_idx, 'weight'] += group.loc[min_weight_idx, 'weight']
        return group.drop(min_weight_idx)
    return group

# Ensure 'weight' is numeric
collected_data['weight'] = pd.to_numeric(collected_data['weight'], errors='coerce')

# Apply the function to each group
collected_data = collected_data.groupby(['consistent_name', 'date'], as_index=False).apply(combine_weights).reset_index(drop=True)

############################################################################
# data manipulation (not in use)
############################################################################
# if a data point on weight is significantely larger or smaller it will be replaced by the previous data point
###### data point will be replace if:
# old_data_point * 1.75 < new_data point
# old_data_point * 0.5 < new_data point

# List of companies to include
included_companies = ['BANCO SANTANDER SA', 'BNP PARIBAS SA', 'TELEFONICA SA', "IBERDROLA SA", "SOCIETE GENERALE SA", "KONINKLIJKE PHILIPS NV", "CREDIT AGRICOLE SA", "LAFARGE SA"]

# Function to replace significant changes in weight within the same company
def replace_significant_changes(group):
    # Check if the company is in the included list
    if group['consistent_name'].iloc[0] not in included_companies:
        return group  # Return the group unchanged if the company is not included
    
    for i in range(1, len(group)):
        old_data_point = group.iloc[i - 1]['weight']  # Previous data point
        new_data_point = group.iloc[i]['weight']      # Current data point
        
        # Replace the new data point if it's significantly larger or smaller than the old data point
        if (old_data_point * 1.75 < new_data_point) or (new_data_point < old_data_point * 0.5):
            group.at[group.index[i], 'weight'] = old_data_point
    return group

# Sort the DataFrame by 'consistent_name' and 'date'
collected_data = collected_data.sort_values(by=['consistent_name', 'date'])

# Apply the function to each group of consistent_name
collected_data = collected_data.groupby('consistent_name').apply(replace_significant_changes)

#collected_data = collected_data.groupby('consistent_name').filter(lambda x: len(x) >= 15)

# Save the collected data to a new CSV file
collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_50_merge_raw.csv', index=False)



