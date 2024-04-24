import pandas as pd
import eikon as ek
import warnings
from datetime import datetime
from pandas import date_range

ek.set_app_key('80631b9534434526bb7b73ad26db914d4c2d9769')

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv')  # Make sure to provide the correct path

# Extract the RICs into a list
ric_list = list(pd.unique(ric_df['stock_RIC'].tolist()))
print(len(ric_list))

########################################################################
def get_first_days(start_date, end_date):
    # Create a date range from start date to end date with monthly frequency, starting at the first day of each month
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    # Format dates as strings and return as a list
    return [date.strftime('%Y-%m-%d') for date in date_range]

# Define the date range
start_date = '2010-01-01'
end_date = '2010-01-01'
first_days = get_first_days(start_date, end_date)
print(first_days)

########################################################################
# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

for sdate_for_year in first_days:
    print(sdate_for_year)
    
    # Assuming the fields are named correctly for the Eikon API
    df, e = ek.get_data(instruments = ric_list,
                   fields = ["TR.FundParentType",
                             "TR.FundInvestorType",
                             "TR.FundInvtStyleCode",
                             #############################################################
                             "TR.FundPortfolioName", 
                             "TR.FundTotalEquityAssets", 
                             "TR.FdAdjSharesHeldValue(SortOrder=Descending)",
                             #############################################################
                             "TR.FdAdjPctOfShrsOutHeld",
                             "TR.FundPctPortfolio",
                             "TR.FundAddrCountry",
                             "TR.FdAdjSharesHeldValue.date"],
                   parameters = {'EndNum':'1000', "SDate": sdate_for_year, "Curn":"EUR", "Scale":6})
    
    df['date'] = sdate_for_year

    # Append the retrieved dataframe to the aggregated dataframe
    aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)

#aggregated_df = aggregated_df.sort_values(by="TR.FdAdjSharesHeldValue", ascending=False)
#aggregated_df.columns = ["stock_RIC", "fund_type", "fund_name", "market_cap_fund", "stock_value_held", "country", "date"]

############################ export data frame
#columns_to_keep = ["stock_RIC", "fund_name", "stock_value_held", "market_cap_fund", "country", "date"]
#aggregated_df = aggregated_df[columns_to_keep]

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\fund_holdings_data\\etf_holdings_600_stocks_test.csv"
aggregated_df.to_csv(file_path, index=False)
