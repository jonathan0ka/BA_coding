import pandas as pd
import eikon as ek
import warnings
import datetime
from datetime import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

# Define the date range
start_date = '2010-01-01'
end_date = '2024-01-01'

# Import the CSV file containing the stock RICs
ric_df = pd.read_csv('C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv')

# get unqiue stock_RIC
ric_list = unique(ric_df['stock_RIC'].tolist())
print(ric_list)

# Initialize an empty DataFrame to aggregate the results
aggregated_df = pd.DataFrame()

fields = ["TR.PriceClose.date",
          "TR.PriceClose(Scale=0)",
          ##########################
          "TR.TotalReturn1D",
          ##########################
          "TR.Volume(Scale = 0)",
          #"TR.Volume.date"
          ##########################
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
          ]

aggregated_df, e = ek.get_data(instruments = ric_list,
                    fields = fields,
                    parameters = {"SDate": "2023-01-01", "EDate": "2024-01-01", "Frq":"D", "Curn":"EUR", "Scale":6})


#"TR.BVPSMean(Period=FY1)"
print(aggregated_df)
print(type(aggregated_df))

# Convert to datetime
aggregated_df['Date'] = pd.to_datetime(df['Date'])

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\stock_level_data\\stock_level_data.csv"
aggregated_df.to_csv(file_path, index=False)
print(f"Data exported successfully")