import pandas as pd
import os

# Load the overview DataFrame
relevant_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')

# Ensure the 'date' column is in datetime format
relevant_df['date'] = pd.to_datetime(relevant_df['date'])

# Sort the relevant_df by 'consistent_name' and 'date' in descending order
# This places the most recent date for each 'consistent_name' at the top
relevant_df = relevant_df.sort_values(by=['consistent_name', 'date'], ascending=[True, False])

# Convert the DataFrame to a dictionary
consistent_ticker_dict = pd.Series(consistent_ticker_mapping.ticker.values, index=consistent_ticker_mapping.consistent_name).to_dict()

# Create a mapping from name to consistent_name
name_to_consistent_name = dict(zip(relevant_df['name'], relevant_df['consistent_name']))