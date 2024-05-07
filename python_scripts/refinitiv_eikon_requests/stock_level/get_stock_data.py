import pandas as pd
import eikon as ek
import warnings
from datetime import datetime

import time
# Record the start time
start_time = time.time()

hsg_24 = "825c77d2d6cb4b3ba41d25d4c0b87c46325cd98c"
uzh_2 = "80631b9534434526bb7b73ad26db914d4c2d9769"
ek.set_app_key(hsg_24)

########################################################################
# dates
########################################################################
def get_first_last_days(start_date_s, end_date_s):
    # Create a date range from start date to end date with monthly frequency, starting at the first day of each month
    date_range_s = pd.date_range(start=start_date_s, end=end_date_s, freq='MS')

    # Calculate the last days by finding the end of the month for each start date
    last_days = (date_range_s + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
    
    formatted_range_s = [date.strftime('%Y-%m-%d') for date in date_range_s]
    formatted_range_e = [date.strftime('%Y-%m-%d') for date in last_days]

    return list(zip(formatted_range_s, formatted_range_e))

dates = get_first_last_days("2019-06-01", '2019-06-30')

########################################################################
# get stock_RIC
########################################################################
# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv')

# get unqiue stock_RIC
ric_df = ric_df.drop_duplicates(subset = "stock_RIC")
ric_list = ric_df['stock_RIC'].tolist()
print(len(ric_list))

########################################################################
# initialize csv file
########################################################################
# Initialize an empty DataFrame to aggregate the results
col_names = ["stock_RIC",
             "date",
             "price",
             "return1D",
             "return1Wk",
             "return1Mo",
             "volume",
             "turnover",
             "market_cap",
             "market_cap_TEST_1",
             "market_cap_TEST_2",
             "gross_profit",
             "price_to_BV",
             "bid_price",
             "ask_price"
            ]

aggregated_df = pd.DataFrame(columns = col_names)

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\stock_level_data\\stock_level_data_TEST_1.csv"
aggregated_df.to_csv(file_path, index=False)

########################################################################
# api request
########################################################################
fields = ["TR.PriceClose.date",
          "TR.PriceClose(Scale=0, Curn=EUR)",
          ########################## returns
          "TR.TotalReturn1D", # last trading day
          "TR.TotalReturn1Wk",
          "TR.TotalReturn1Mo", # lag 30 days
          #"TR.Volatility30D",
          #"TR.Volatility60D",
          #"TR.Volatility90D",
          #"TR.Volatility250D",
          ########################## volume
          "TR.Volume",
          #"TR.Volume.date"
          "TR.TURNOVER(Curn=EUR)",
          #"TR.TURNOVER.date",
          ##########################
          "TR.CompanyMarketCap(Curn=EUR)",
          "TR.CompanyMarketCapitalization(Curn=EUR)",
          "TR.IssueMarketCap",
          ##########################
          "TR.GrossProfit(Period=FY0)", 
          "TR.PriceToBVPerShare", #book value / price
          "TR.BIDPRICE(Curn=EUR)", #last bidprice of previous day
          "TR.ASKPRICE(Curn=EUR)", #last askprice of previous day
          #"TR.ASKPRICE.date"
          ##########################
          #"TR.HeadquartersCountry",
          #"TR.IssuerRating",
          #"TR.WACC",
          #"TR.TotalEquity(Period=FY0)",
          ##########################
          #"TR.TotalAssetsActual(Period=FY0)", #total assets - (total liabilties - prefered stock)
          #"TR.TotalLiabilities(Period=FY0)",
          #"TR.PreferredStockNet(Period=FY0)"

          ]

for date_tuple in dates:
    print(f"Starting with data retrival for {date_tuple[0]} to {date_tuple[1]}")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        aggregated_df, e = ek.get_data(instruments = ric_list,
                                       fields = fields,
                                       parameters = {"SDate": date_tuple[0], "EDate": date_tuple[1], "Frq":"D"})
    
        # Append DataFrame to an existing CSV file
        aggregated_df.columns = col_names
        aggregated_df.to_csv(file_path, mode='a', header=False, index=False)
    print(f"Sucessfull retrival till {date_tuple[1]}")

print(f"Data exported successfully")


# # Convert to datetime
#aggregated_df['Date'] = pd.to_datetime(aggregated_df['Date']).dt.date
#aggregated_df['Date'] = pd.to_datetime(aggregated_df['Date'])


