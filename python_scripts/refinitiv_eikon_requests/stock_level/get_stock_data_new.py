import pandas as pd
import eikon as ek
import warnings
import time
from datetime import datetime

# Constants
HSG_24_API_KEY = "825c77d2d6cb4b3ba41d25d4c0b87c46325cd98c"
UZH_2_API_KEY = "80631b9534434526bb7b73ad26db914d4c2d9769"
EXPORT_CSV_FILE_PATH = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\stock_level_data\\stock_level_data_countries.csv"
RIC_FILE_PATH = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv"

def get_first_last_days(start_date_s, end_date_s):
    date_range_s = pd.date_range(start=start_date_s, end=end_date_s, freq='MS')
    last_days = (date_range_s + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
    formatted_range_s = [date.strftime('%Y-%m-%d') for date in date_range_s]
    formatted_range_e = [date.strftime('%Y-%m-%d') for date in last_days]
    return list(zip(formatted_range_s, formatted_range_e))

def read_and_process_ric_data():
    ric_df = pd.read_csv(RIC_FILE_PATH)
    ric_df = ric_df.drop_duplicates(subset="stock_RIC")
    return ric_df['stock_RIC'].tolist()

def initialize_csv(col_names, first_time = FALSE):
    aggregated_df = pd.DataFrame(columns=col_names)
    aggregated_df.to_csv(CSV_FILE_PATH, index=False)

def fetch_and_store_data(fields, ric_list, dates):
    for date_tuple in dates:
        print(f"Starting data retrieval for {date_tuple[0]} to {date_tuple[1]}")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)
            df, _ = ek.get_data(instruments=ric_list, fields=fields,
                                parameters={"SDate": date_tuple[0], "EDate": date_tuple[1], "Frq":"D"})
            df.columns = col_names
            df.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)
        print(f"Successful retrieval till {date_tuple[1]}")

def api_request(API_KEY, start_date, end_date, col_names, fields):
    start_time = time.time()
    ek.set_app_key(API_KEY)
    dates = get_first_last_days(start_date, end_date)
    ric_list = read_and_process_ric_data()
    initialize_csv(col_names, first_time = True)

    fetch_and_store_data(fields, ric_list, dates)
    print(f"Data exported successfully in {time.time() - start_time} seconds")


########## original request
col_names = ["stock_RIC", "date", "price", "return1D", "return1Wk", "return1Mo", "volume",
                 "turnover", "market_cap", "market_cap_TEST_1", "market_cap_TEST_2", "gross_profit",
                 "price_to_BV", "bid_price", "ask_price"]

fields = ["TR.PriceClose.date", "TR.PriceClose(Scale=0, Curn=EUR)", "TR.TotalReturn1D", "TR.TotalReturn1Wk",
              "TR.TotalReturn1Mo", "TR.Volume", "TR.TURNOVER(Curn=EUR)", "TR.CompanyMarketCap(Curn=EUR)",
              "TR.CompanyMarketCapitalization(Curn=EUR)", "TR.IssueMarketCap", "TR.GrossProfit(Period=FY0)",
              "TR.PriceToBVPerShare", "TR.BIDPRICE(Curn=EUR)", "TR.ASKPRICE(Curn=EUR)"]

#api_request(HSG_24_API_KEY, "2019-06-01", '2019-06-30', col_names, fields)

########## country request for RICs
col_names = ["stock_RIC", "headquarters_country", "exchange_country", "business_sector", "economic_sector"]
fields = ["TR.HeadquartersCountry", "TR.ExchangeCountry", "TR.TRBCBusinessSector", "TR.TRBCEconomicSector"]

api_request(HSG_24_API_KEY, "2023-01-01", '2023-01-01', col_names, fields)