# test change
# test change windows

# 600 --> 2000
# 100 * 2000 * 365


# import pandas as pd
# import eikon as ek
# import warnings

# warnings.simplefilter(action='ignore', category=FutureWarning)
# ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')
# #df, err = ek.get_timeseries(["CN10YT=RR"], start_date=datetime(1997, 1, 1), end_date=datetime(2022, 3, 29), interval='monthly')


# # Define the date range
# start_date = '2024-01-01'
# end_date = '2024-03-01'

# # Use CustomBusinessMonthEnd to generate the last business day of each month
# business_days = pd.date_range(start=start_date, end=end_date, freq='BM')

# # Prepare to collect data
# data_list = []

# # Loop through each business day and make the API request
# for date in business_days:
#     try:
#         date_str = date.strftime('%Y-%m-%d')
#         data, err = ek.get_data(
#             instruments = ['.STOXX'],
#             fields = ['TR.HoldingsDate', 'TR.ETPConstituentRIC', 'TR.MarketCap'],
#             parameters = {'SDate': date_str}
#         )
        
#         # Check for an error from the API call
#         if err is not None:
#             print(f"Error fetching data for {date_str}: {err}")
#         elif data is not None:
#             data['date'] = date_str  # Add the date to the DataFrame
#             data_list.append(data)  # Append the DataFrame to the list
#     except Exception as e:
#         print(f"An exception occurred for {date_str}: {e}")

# # Concatenate all data into a single DataFrame
# if data_list:
#     df_final = pd.concat(data_list, ignore_index=True)
#     # Rename the columns as needed
#     df_final = df_final.rename(columns={'TR.HoldingsDate': 'date', 'TR.ETPConstituentRIC': 'ticker', 'TR.MarketCap': 'market_cap'})

#     # Specify your path
#     file_path = 'D:\\30_One_Drive_Jonathan\\OneDrive\\RStudio\\BA_Thesis\\python_scripts\\refinitiv_eikon_requests\\constituent_list_euro_stoxx_600.csv'
    
#     # Export the DataFrame to a CSV file
#     df_final.to_csv(file_path, index=False)

#     print("Data exported successfully.")
# else:
#     print("No data to export.")


##################################################################################
# gpt
##################################################################################
# import pandas as pd
# import eikon as ek
# import warnings

# warnings.simplefilter(action='ignore', category=FutureWarning)
# ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')
# #df, err = ek.get_timeseries(["CN10YT=RR"], start_date=datetime(1997, 1, 1), end_date=datetime(2022, 3, 29), interval='monthly')


# # Define the date range
# start_date = '2024-01-01'
# end_date = '2024-03-01'

# # Generate the last business day of each month within the date range
# business_days = pd.date_range(start=start_date, end=end_date, freq='BM')

# all_data = []

# # Loop through each business day and make the API requests
# for date in business_days:
#     date_str = date.strftime('%Y-%m-%d')
#     print("Processing date:", date_str)
    
#     # First API call to get the constituents
#     constituents, err = ek.get_data(
#         instruments = ['.STOXX'],
#         fields = ['TR.IndexConstituentRIC', 'TR.IndexConstituentName'],
#         parameters = {'SDate': date_str}
#     )

#     if constituents is not None and not constituents.empty:
#         # Second API call to get additional data for each constituent
#         market_cap_data, err = ek.get_data(
#             instruments = constituents['Constituent RIC'].tolist(),
#             fields = ['TR.CompanyMarketCap'],
#             parameters = {'SDate': date_str}
#         )

#         # Combine the data from both calls
#         if market_cap_data is not None and not market_cap_data.empty:
#             combined_data = pd.merge(constituents, market_cap_data, on='Instrument', how='left')
#             combined_data['Date'] = date_str  # Add the date to the combined data
#             all_data.append(combined_data)

# # Concatenate all data into a single DataFrame
# if all_data:
#     df_final = pd.concat(all_data, ignore_index=True)
    
#     # Specify your path
#     file_path = 'D:\\30_One_Drive_Jonathan\\OneDrive\\RStudio\\BA_Thesis\\python_scripts\\refinitiv_eikon_requests\\constituent_list_euro_stoxx_600.csv'
    
#     # Export the DataFrame to a CSV file
#     df_final.to_csv(file_path, index=False)

#     print("Data exported successfully.")
# else:
#     print("No data to fetch or export.")


################################################################################
# v1
################################################################################
import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Define the date range
start_date = '2024-01-01'
end_date = '2024-03-01'

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
    
    if err:
        print(f"Error fetching data: {err}")
        continue  # Skip this iteration if there's an error

    #print(df)

    # Get the market cap for each constituent
    df2, err = ek.get_data(
        instruments=df['Constituent RIC'].tolist(),
        fields=['TR.CompanyMarketCap'],
        parameters={'SDate': sdate_for_year}
    )
    
    if err:
        print(f"Error fetching data: {err}")
        continue  # Skip this iteration if there's an error

    #print(df2)

    # Data Preparation: Ensure no leading/trailing whitespaces
    # df['Constituent RIC'] = df['Constituent RIC'].str.strip()
    # df2['Constituent RIC'] = df2['Constituent RIC'].str.strip()

    # Merge the dataframes
    #merged_df = pd.merge(df, df2, on='Constituent RIC', how='left')

    # Merge the two dataframes on the 'TR.IndexConstituentRIC' column
    merged_df = pd.merge(df, df2, left_on='Constituent RIC', right_on='Instrument', how='left')
    
    # Add the date column to the merged dataframe
    merged_df['Date'] = sdate_for_year
    
    # Append the merged dataframe to the aggregated dataframe
    aggregated_df = pd.concat([aggregated_df, merged_df], ignore_index=True)

    #columns_to_keep = ["Constituent RIC", "Constituent Name", "Company Market Cap", "Date"]
    #aggregated_df = aggregated_df[columns_to_keep]

# Now aggregated_df contains all the merged data with the date column
# print(aggregated_df)


file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\constituents_stoxx_europe_600"
aggregated_df.to_csv(file_path, index=False)
print(f"Data exported successfully")