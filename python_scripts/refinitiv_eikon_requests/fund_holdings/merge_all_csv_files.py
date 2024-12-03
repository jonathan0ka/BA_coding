import pandas as pd
import os

def merge_csv_files(directory_path, output_file):
    dataframes = []

    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
           
            file_path = os.path.join(directory_path, filename)
            print(file_path)
            df = pd.read_csv(file_path)
            dataframes.append(df)

    merged_df = pd.concat(dataframes, ignore_index=True)

    merged_df.to_csv(output_file, index=False)
    print(f"All CSV files have been merged into {output_file}")


raw_data_path = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/fund_holdings_data/raw_data'
merged_data_path = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/fund_holdings_data/merged_data.csv'
merge_csv_files(raw_data_path, merged_data_path)