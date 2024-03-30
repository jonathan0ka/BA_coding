import pandas as pd

df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_50_overview_raw.csv')

unique_names = df['consistent_name'].unique()

unique_names_df = pd.DataFrame(unique_names, columns=['consistent_name'])

output_file_path = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/euro_stoxx_50/euro_stoxx_50_unique_stocks.csv'  # Replace this with the desired output path
unique_names_df.to_csv(output_file_path, index=False)
