import pandas as pd

# Read data
df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/constituents_stoxx_europe_600_merge.csv')

# Create pivot table
pivot_df = df.pivot_table(index='date', columns='stock_RIC', aggfunc='size', fill_value=0)

# Convert to long format
pivot_df.reset_index(inplace=True)
long_df = pivot_df.melt(id_vars=['date'], var_name='stock_RIC', value_name='member')

# Adjust the 'member' column to be binary
long_df['member'] = (long_df['member'] > 0).astype(int)

# Print the resulting DataFrame
long_df.columns = ["date", "stock_RIC", "index_member"]
print(long_df)

################### changing dates from end of month to start of next month
def adjust_date_to_next_month_first(input_df, date_column):

    # Read the CSV file into a DataFrame
    df = input_df
    # Ensure the date column is in datetime format
    df[date_column] = pd.to_datetime(df[date_column])

    # Adjust the date to the first day of the following month
    df[date_column] = df[date_column] + pd.DateOffset(days=1)
    df[date_column] = df[date_column] - pd.to_timedelta(df[date_column].dt.day - 1, unit='d')

    # Write the updated DataFrame back to a new CSV file
    return df
    print(f"Dates in column '{date_column}' have been adjusted and saved to {output_file}")

long_df = adjust_date_to_next_month_first(long_df, "date")

file_path = "/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/formated_constituents_stoxx_europe_600_with_ranking.csv"
long_df.to_csv(file_path, index=False)
