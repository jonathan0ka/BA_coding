import pandas as pd
import eikon as ek
import warnings
from datetime import datetime

########################################################################
# setup
########################################################################
import time
# Record the start time
start_time = time.time()

ek.set_app_key('9aceb0f0b92f4b5cab82266c64eee1e83614934e')

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv')

# get unqiue stock_RIC
ric_df = ric_df.drop_duplicates(subset = "stock_RIC")
ric_list = ric_df['stock_RIC'].tolist()
print(len(ric_list))

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

########################################################################
# dates
########################################################################
def get_first_days(start_date, end_date):
    # Create a date range from start date to end date with monthly frequency, starting at the first day of each month
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    # Format dates as strings and return as a list
    return [date.strftime('%Y-%m-%d') for date in date_range]

# Define the date range
start_date = '2024-01-01'
end_date = '2024-02-01'
first_days = get_first_days(start_date, end_date)

########################################################################
# api request
########################################################################
fields = ["TR.PriceClose.date",
          "TR.PriceClose(Scale=0)",
          ########################## returns
          "TR.TotalReturn1D", # last trading day
          "TR.TotalReturn1Mo", # lag 30 days
          "TR.TotalReturn1Wk",
          "TR.Volatility30D",
          "TR.Volatility60D",
          "TR.Volatility90D",
          "TR.Volatility250D",
          ########################## volume
          "TR.Volume(Scale = 0)",
          #"TR.Volume.date"
          "TR.TURNOVER",
          #"TR.TURNOVER.date",
          ##########################
          "TR.CompanyMarketCap",
          #"TR.CompanyMarketCap.date",
          ##########################
          "TR.GrossProfit(Period=FY0)", 
          "TR.PriceToBVPerShare", #book value / price
          "TR.BIDPRICE", #last bidprice of previous day
          "TR.ASKPRICE", #last askprice of previous day
          #"TR.ASKPRICE.date"
          ##########################
          "TR.HeadquartersCountry",
          #"TR.IssuerRating",
          "TR.WACC",
          "TR.TotalEquity(Period=FY0)",
          ##########################
          "TR.TotalAssetsActual(Period=FY0)", #total assets - (total liabilties - prefered stock)
          "TR.TotalLiabilities(Period=FY0)",
          "TR.PreferredStockNet(Period=FY0)"

          ]

for sdate_for_month in first_days:
    print(sdate_for_month)
    df, e = ek.get_data(instruments = ric_list,
                    fields = fields,
                    parameters = {"SDate": sdate_for_month, "Curn":"EUR", "Scale":6})
    
    df['date'] = sdate_for_month

    # Append the retrieved dataframe to the aggregated dataframe
    aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\stock_level_data\\m_stock_level_data.csv"
aggregated_df.to_csv(file_path, index=False)
print(f"Data exported successfully")
    