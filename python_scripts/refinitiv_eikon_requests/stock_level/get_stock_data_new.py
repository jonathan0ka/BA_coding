import os
import logging
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import eikon as ek
import warnings
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_fetch_process.log"),
        logging.StreamHandler()
    ]
)

# Configuration dictionary
CONFIG = {
    "API_KEYS": {
        "HSG_24": "XXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "UZH_2": "XXXXXXXXXXXXXXXXXXXXXXXXXXX"
    },
    "FILE_PATHS": {
        "CSV_FILE_PATH": Path("C:/Users/Shadow/OneDrive/BA_Thesis/BA_coding/datasets/eikon_data/stock_level_data/stock_level_data_countries.csv"),
        "RIC_FILE_PATH": Path("C:/Users/Shadow/OneDrive/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/formated_constituents_stoxx_europe_600.csv")
    },
    "FETCH_SETTINGS": {
        "INITIALIZE_FIRST_TIME": True
    }
}

def get_first_last_days(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    try:
        date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
        last_days = (date_range + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
        formatted_range = [(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')) for start, end in zip(date_range, last_days)]
        logging.debug(f"Generated date ranges: {formatted_range}")
        return formatted_range
    except Exception as e:
        logging.error(f"Error generating date ranges: {e}", exc_info=True)
        return []

def read_and_process_ric_data(ric_file_path: Path) -> List[str]:
    try:
        ric_df = pd.read_csv(ric_file_path)
        ric_df = ric_df.drop_duplicates(subset="stock_RIC")
        ric_list = ric_df['stock_RIC'].dropna().tolist()
        logging.info(f"Processed {len(ric_list)} unique RICs from {ric_file_path}")
        return ric_list
    except FileNotFoundError:
        logging.error(f"RIC file not found at {ric_file_path}")
        return []
    except Exception as e:
        logging.error(f"Error processing RIC data: {e}", exc_info=True)
        return []

def initialize_csv(csv_file_path: Path, col_names: List[str], first_time: bool = False) -> None:
    try:
        if first_time or not csv_file_path.exists():
            aggregated_df = pd.DataFrame(columns=col_names)
            aggregated_df.to_csv(csv_file_path, index=False)
            logging.info(f"Initialized CSV file with columns: {col_names}")
        else:
            logging.info(f"CSV file {csv_file_path} already exists. Skipping initialization.")
    except Exception as e:
        logging.error(f"Error initializing CSV file {csv_file_path}: {e}", exc_info=True)

def fetch_and_store_data(fields: List[str], ric_list: List[str], dates: List[Tuple[str, str]], 
                        csv_file_path: Path, col_names: List[str]) -> None:
    for start_date, end_date in dates:
        logging.info(f"Starting data retrieval for {start_date} to {end_date}")
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                df, err = ek.get_data(
                    instruments=ric_list, 
                    fields=fields,
                    parameters={
                        "SDate": start_date, 
                        "EDate": end_date, 
                        "Frq": "D"
                    }
                )
                if err:
                    logging.error(f"Error fetching data for {start_date} to {end_date}: {err}")
                    continue

                if df.empty:
                    logging.warning(f"No data retrieved for {start_date} to {end_date}")
                    continue

                df.columns = col_names
                df.to_csv(csv_file_path, mode='a', header=False, index=False)
                logging.info(f"Successfully retrieved and stored data till {end_date}")
        except Exception as e:
            logging.error(f"Exception during data fetch for {start_date} to {end_date}: {e}", exc_info=True)

def api_request(api_key: str, start_date: str, end_date: str, 
               col_names: List[str], fields: List[str], 
               csv_file_path: Path) -> None:
    start_time = time.time()
    try:
        ek.set_app_key(api_key)
        dates = get_first_last_days(start_date, end_date)
        ric_list = read_and_process_ric_data(CONFIG["FILE_PATHS"]["RIC_FILE_PATH"])
        initialize_csv(csv_file_path, col_names, first_time=CONFIG["FETCH_SETTINGS"]["INITIALIZE_FIRST_TIME"])
        fetch_and_store_data(fields, ric_list, dates, csv_file_path, col_names)
        elapsed_time = time.time() - start_time
        logging.info(f"Data exported successfully in {elapsed_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error during API request: {e}", exc_info=True)

def main():
    # Define different data requests
    data_requests = [
        {
            "description": "Original Request",
            "col_names": [
                "stock_RIC", "date", "price", "return1D", "return1Wk", "return1Mo", 
                "volume", "turnover", "market_cap", "market_cap_TEST_1", 
                "market_cap_TEST_2", "gross_profit", "price_to_BV", 
                "bid_price", "ask_price"
            ],
            "fields": [
                "TR.PriceClose.date", 
                "TR.PriceClose(Scale=0, Curn=EUR)", 
                "TR.TotalReturn1D", 
                "TR.TotalReturn1Wk",
                "TR.TotalReturn1Mo", 
                "TR.Volume", 
                "TR.TURNOVER(Curn=EUR)", 
                "TR.CompanyMarketCap(Curn=EUR)",
                "TR.CompanyMarketCapitalization(Curn=EUR)", 
                "TR.IssueMarketCap", 
                "TR.GrossProfit(Period=FY0)",
                "TR.PriceToBVPerShare", 
                "TR.BIDPRICE(Curn=EUR)", 
                "TR.ASKPRICE(Curn=EUR)"
            ],
            "api_key": CONFIG["API_KEYS"]["HSG_24"],
            "start_date": "2019-06-01",
            "end_date": "2019-06-30",
            "csv_file_path": CONFIG["FILE_PATHS"]["CSV_FILE_PATH"]
        },
        {
            "description": "Country Request for RICs",
            "col_names": [
                "stock_RIC", "headquarters_country", 
                "exchange_country", "business_sector", "economic_sector"
            ],
            "fields": [
                "TR.HeadquartersCountry", 
                "TR.ExchangeCountry", 
                "TR.TRBCBusinessSector", 
                "TR.TRBCEconomicSector"
            ],
            "api_key": CONFIG["API_KEYS"]["HSG_24"],
            "start_date": "2023-01-01",
            "end_date": "2023-01-01",
            "csv_file_path": CONFIG["FILE_PATHS"]["CSV_FILE_PATH"]
        }
    ]

    # Execute each data request
    for request in data_requests:
        logging.info(f"Executing data request: {request['description']}")
        api_request(
            api_key=request["api_key"],
            start_date=request["start_date"],
            end_date=request["end_date"],
            col_names=request["col_names"],
            fields=request["fields"],
            csv_file_path=request["csv_file_path"]
        )

if __name__ == "__main__":
    main()
