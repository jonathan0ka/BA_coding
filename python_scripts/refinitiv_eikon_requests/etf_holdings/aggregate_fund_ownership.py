import pandas as pd

# Load the data
df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/etf_holdings_600_stocks.csv')

# Group by 'stock_RIC' and 'date', and sum the 'stock_value_held'
grouped_df = df.groupby(['stock_RIC', 'date'])['stock_value_held'].sum().reset_index()

# Export to new CSV
file_path = "/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/aggregated_holdings_600_stocks.csv"
grouped_df.to_csv(file_path, index=False)