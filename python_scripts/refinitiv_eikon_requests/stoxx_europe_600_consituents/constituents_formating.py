import pandas as pd

# Read data
df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/constituents_stoxx_europe_600_merge.csv')

# Create pivot table
pivot_df = df.pivot_table(index='Date', columns='Constituent RIC', aggfunc='size', fill_value=0)

# Convert to long format
pivot_df.reset_index(inplace=True)
long_df = pivot_df.melt(id_vars=['Date'], var_name='stock_RIC', value_name='member')

# Adjust the 'member' column to be binary
long_df['member'] = (long_df['member'] > 0).astype(int)

# Print the resulting DataFrame
print(long_df)

long_df.columns = ["date", "stock_RIC", "index_member"]

# Save to CSV (optional)
file_path = "/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/formated_constituents_stoxx_europe_600.csv"
long_df.to_csv(file_path, index=False)
