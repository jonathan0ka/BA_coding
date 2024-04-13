import pandas as pd
import eikon as ek
import warnings

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Define the date range
start_date = '2024-01-01'
end_date = '2024-02-01'

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

# Generate the last business day of each month within the date range
business_days = pd.date_range(start=start_date, end=end_date, freq="BM")

for specific_date in business_days:
    sdate_for_year = specific_date.strftime("%Y-%m-%d")
    print(sdate_for_year)
    
    # Get the constituents of the index
    df, err = ek.get_data(
        instruments=['.STOXX'],
        fields=['TR.IndexConstituentRIC', 'TR.IndexConstituentName'],
        parameters={'SDate': sdate_for_year}
    )

    if err is None:
        # Add the date column to the merged dataframe
        df['Date'] = sdate_for_year
        
        # Append the merged dataframe to the aggregated dataframe
        aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)
    else:
        warnings.warn(f"Error retrieving data for {sdate_for_year}: {err}")

columns_to_keep = ["Constituent RIC", "Constituent Name", "Date"]
aggregated_df = aggregated_df[columns_to_keep]

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\constituents_stoxx_europe_600.csv"
aggregated_df.to_csv(file_path, index=False)
print(f"Data exported successfully")
