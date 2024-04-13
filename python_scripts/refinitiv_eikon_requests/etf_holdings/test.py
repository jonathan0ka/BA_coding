import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Define the date range
start_date = '2024-01-01'
end_date = '2024-03-01'

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/constituents_stoxx_europe_600.csv')  # Make sure to provide the correct path

# Extract the RICs into a list
ric_list = ric_df['Constituent RIC'].tolist()

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

# Generate the last business day of each month within the date range
business_days = pd.date_range(start=start_date, end=end_date, freq="BM")

for specific_date in business_days:
    sdate_for_year = specific_date.strftime("%Y-%m-%d")
    print(sdate_for_year)
    
    # Assuming the fields are named correctly for the Eikon API
    df, e = ek.get_data(instruments=ric_list,
                        fields=["TR.FundInvestorType", "TR.FundPortfolioName", 
                                "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue", 
                                "TR.FundAddrCountry"],
                        parameters={"date": sdate_for_year, "currency": "EUR", "scale": "6", "count": 10})
    
    df['date'] = sdate_for_year

    # Append the retrieved dataframe to the aggregated dataframe
    aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)

# Sort and rename the columns for the aggregated dataframe
aggregated_df = aggregated_df.sort_values(by="TR.FdAdjSharesHeldValue", ascending=False)
aggregated_df.columns = ["stock_RIC", "fund_type", "fund_name", "stock_value_held", "market_cap_fund", "country", "date"]

print(df)
# "TR.FundParentType", "TR.FundInvestorType", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue", "TR.FundAddrCountry",TR.FundAdjShrsHeld, TR.FdAdjSharesHeldValue
