import os
from datetime import datetime
import pandas as pd

# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/ishares_data/ishares_raw_data/stoxx_europe_600_downloads'

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
        
        
        # Append the filtered and updated DataFrame to the list
        data_frames.append(current_df)

# Concatenate all data frames
collected_data = pd.concat(data_frames, ignore_index=True)
collected_data.columns = ["ticker", "name", "sector", "asset_class","market_cap", "weight", "nominal_value", "nominal", "price", "country", "stock_exchange","currency","date"]

# Save the collected data to a new CSV file
collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/ishares_data/ishares_merged_data/stoxx_europe_600/stoxx_europe_600_data.csv', index=False)
