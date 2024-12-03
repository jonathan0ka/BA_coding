# # Load the name mapping from a CSV file
# mapping_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')
# # Create a dictionary from the dataframe where keys are consistent names and values are lists of names
# name_mapping = {k: g["name"].tolist() for k, g in mapping_df.groupby("consistent_name")}

# # Function to replace the name in Python, using the mapping
# def replace_name(name, name_mapping):
#     for consistent_name, names in name_mapping.items():
#         if name in names:
#             return consistent_name
#     return name


# import pandas as pd
# import os

# relevant_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')

# # Specify the directory containing the 5000 CSV files
# directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'

# # Create a mapping from name to consistent_name
# name_to_consistent_name = dict(zip(relevant_df['name'], relevant_df['consistent_name']))

# # List of unique names to filter by
# relevant_names = relevant_df['name'].unique()

# # Initialize an empty list to store the data frames
# data_frames = []

# # Loop through all the CSV files in the directory
# for filename in os.listdir(directory):
#     if filename.endswith('.csv'):
#         # Extract the date from the filename
#         date_str = filename[-12:-4]
#         # Construct the full path to the file
#         file_path = os.path.join(directory, filename)
#         # Read the current CSV file
#         current_df = pd.read_csv(file_path, header=2)
#         # Add the date to the current DataFrame
#         current_df['date'] = pd.to_datetime(date_str, format='%Y%m%d')
#         # Filter to keep only rows with names that are in the relevant_names list
#         current_df = current_df[current_df['Name'].isin(relevant_names)]
#         # Append the filtered DataFrame to the list
#         data_frames.append(current_df)

# # Concatenate all data frames
# collected_data = pd.concat(data_frames, ignore_index=True)

# # Save the collected data to a new CSV file in long format
# collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_merge_raw.csv', index=False)




###### test
# import pandas as pd
# import os

# # Load the overview DataFrame
# relevant_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')

# # Create a mapping from name to consistent_name
# name_to_consistent_name = dict(zip(relevant_df['name'], relevant_df['consistent_name']))

# # Specify the directory containing the CSV files
# directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'

# # Initialize an empty list to store the DataFrames
# data_frames = []

# # Loop through all the CSV files in the directory
# for filename in os.listdir(directory):
#     if filename.endswith('.csv'):
#         file_path = os.path.join(directory, filename)
#         current_df = pd.read_csv(file_path, header=2)
        
#         # Extract the date from the filename and add it as a new column
#         date_str = filename[-12:-4]
#         current_df['date'] = pd.to_datetime(date_str, format='%Y%m%d')
        
#         # Filter the DataFrame based on the names in the overview file
#         current_df = current_df[current_df['Name'].isin(name_to_consistent_name.keys())]
        
#         # Add the 'consistent_name' column based on the 'Name' column mapping
#         current_df['consistent_name'] = current_df['Name'].apply(lambda x: name_to_consistent_name[x])
        
#         # Append the filtered and updated DataFrame to the list
#         data_frames.append(current_df)

# # Concatenate all data frames
# collected_data = pd.concat(data_frames, ignore_index=True)

# # Save the collected data to a new CSV file
# collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_merge_raw.csv', index=False)


##########
import pandas as pd
import os

# Load the overview DataFrame
relevant_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')

# Ensure the 'date' column is in datetime format
relevant_df['date'] = pd.to_datetime(relevant_df['date'])

# Sort the relevant_df by 'consistent_name' and 'date' in descending order
# This places the most recent date for each 'consistent_name' at the top
relevant_df = relevant_df.sort_values(by=['consistent_name', 'date'], ascending=[True, False])

# Drop duplicates to get the most recent 'ticker' for each 'consistent_name'
consistent_ticker_mapping = relevant_df.drop_duplicates(subset='consistent_name')[['consistent_name', 'ticker']]

# Convert the DataFrame to a dictionary
consistent_ticker_dict = pd.Series(consistent_ticker_mapping.ticker.values, index=consistent_ticker_mapping.consistent_name).to_dict()

# Create a mapping from name to consistent_name
name_to_consistent_name = dict(zip(relevant_df['name'], relevant_df['consistent_name']))

# Specify the directory containing the CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'

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
        current_df['consistent_ticker'] = current_df['consistent_name'].apply(lambda x: consistent_ticker_dict[x])
        
        # Append the filtered and updated DataFrame to the list
        data_frames.append(current_df)

# Concatenate all data frames
collected_data = pd.concat(data_frames, ignore_index=True)



# Save the collected data to a new CSV file
collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_merge_raw.csv', index=False)