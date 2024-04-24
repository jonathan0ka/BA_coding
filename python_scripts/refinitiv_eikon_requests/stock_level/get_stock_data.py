import pandas as pd
import eikon as ek
import warnings
from datetime import datetime

import time
# Record the start time
start_time = time.time()


ek.set_app_key('9aceb0f0b92f4b5cab82266c64eee1e83614934e')

# Define the date range
start_date = '2010-01-01'
end_date = '2024-01-01'

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv')

# get unqiue stock_RIC
ric_df = ric_df.drop_duplicates(subset = "stock_RIC")
ric_list = ric_df['stock_RIC'].tolist()
print(len(ric_list))

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

fields = ["TR.PriceClose.date",
          "TR.PriceClose(Scale=0, Curn=EUR)",
          ########################## returns
          "TR.TotalReturn1D", # last trading day
          "TR.TotalReturn1Mo", # lag 30 days
          "TR.TotalReturn1Wk",
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
          #"TR.CompanyMarketCap.date",
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

aggregated_df, e = ek.get_data(instruments = ric_list,
                    fields = fields,
                    parameters = {"SDate": start_date, "EDate": end_date, "Frq":"D"})


# # Convert to datetime
#aggregated_df['Date'] = pd.to_datetime(aggregated_df['Date']).dt.date
#aggregated_df['Date'] = pd.to_datetime(aggregated_df['Date'])


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Runtime of the script is {elapsed_time} seconds")

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\stock_level_data\\stock_level_data.csv"
aggregated_df.to_csv(file_path, index=False)
print(f"Data exported successfully")