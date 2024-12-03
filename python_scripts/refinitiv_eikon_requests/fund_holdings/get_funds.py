import pandas as pd
import eikon as ek
import warnings
import logging
import configparser
from datetime import datetime
from pandas import date_range
from requests.exceptions import HTTPError, RequestException, Timeout
import csv
import os
import sys

# Setup logging
logging.basicConfig(
    filename='data_fetch.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Load configuration
config = configparser.ConfigParser()
config_path = 'config.ini'

if not os.path.exists(config_path):
    logging.error(f"Configuration file {config_path} not found.")
    sys.exit(f"Configuration file {config_path} not found.")

config.read(config_path)

# Retrieve API key and file paths from config
try:
    app_key = config['API']['app_key']
    mac_path = config['Paths']['mac_path']
    windows_path = config['Paths']['windows_path']
    date_tracking_path = config['Paths']['date_tracking_path']
    output_file_path = config['Paths']['output_file_path']
except KeyError as e:
    logging.error(f"Missing configuration for {e}")
    sys.exit(f"Missing configuration for {e}")

# Set Eikon API key
ek.set_app_key(app_key)

##############################################################
# Global Variables
##############################################################
stop_now = False
stop_message = "Critical error: Connection Lost"

def check_conditions(df_tmp):
    global stop_now
    if df_tmp is None:
        stop_now = True
        logging.critical(stop_message)
        print(stop_message)

##############################################################
# Split stock_RIC into 10 equally large lists
##############################################################

def load_ric_list(file_path):
    if not os.path.exists(file_path):
        logging.error(f"RIC file {file_path} does not exist.")
        sys.exit(f"RIC file {file_path} does not exist.")
    
    ric_df = pd.read_csv(file_path)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        ric_list = ric_df['stock_RIC'].dropna().unique().tolist()
    logging.info(f"Loaded {len(ric_list)} unique RICs.")
    return ric_list

def split_list_into_chunks(lst, num_chunks):
    size_each = len(lst) // num_chunks
    chunks = {}
    for i in range(num_chunks):
        start = i * size_each
        end = start + size_each if i < num_chunks - 1 else len(lst)
        chunks[f'list_{i + 1}'] = lst[start:end]
    logging.info(f"Split RICs into {num_chunks} chunks.")
    return chunks

##############################################################
# Date Functions
##############################################################

def get_first_days(start_date, end_date):
    try:
        dr = pd.date_range(start=start_date, end=end_date, freq='MS')
        first_days = [date.strftime('%Y-%m-%d') for date in dr]
        logging.info(f"Generated {len(first_days)} first days between {start_date} and {end_date}.")
        return first_days
    except Exception as e:
        logging.error(f"Error generating date range: {e}")
        sys.exit(f"Error generating date range: {e}")

##############################################################
# Date Storage Management
##############################################################

def read_date_tracking(file_path):
    if not os.path.exists(file_path):
        logging.error(f"Date tracking file {file_path} does not exist.")
        sys.exit(f"Date tracking file {file_path} does not exist.")
    
    try:
        with open(file_path, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)
            row = next(csv_reader)
            start_date, end_date = row[0], row[1]
            logging.info(f"Read start_date: {start_date}, end_date: {end_date} from date tracking.")
            return start_date, end_date
    except Exception as e:
        logging.error(f"Error reading date tracking file: {e}")
        sys.exit(f"Error reading date tracking file: {e}")

def update_csv(file_path, new_value, row_index=1, column_index=0):
    try:
        data = []
        with open(file_path, mode='r', newline='') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)
        
        if row_index >= len(data) or column_index >= len(data[row_index]):
            logging.error("Row or column index out of range while updating CSV.")
            return
        
        data[row_index][column_index] = new_value
        
        with open(file_path, mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(data)
        
        logging.info("date_tracking.csv has been successfully updated.")
    except Exception as e:
        logging.error(f"Error updating CSV: {e}")

##############################################################
# Initialize DataFrame
##############################################################

def initialize_output_file(file_path, columns):
    if not os.path.exists(file_path):
        try:
            df = pd.DataFrame(columns=columns)
            df.to_csv(file_path, index=False)
            logging.info(f"Initialized output file at {file_path}.")
        except Exception as e:
            logging.error(f"Error initializing output file: {e}")
            sys.exit(f"Error initializing output file: {e}")

##############################################################
# API Call Function
##############################################################

def fetch_data(value, sdate_for_year, max_retries=3):
    attempts = 0
    while attempts < max_retries:
        try:
            df_tmp, _ = ek.get_data(
                instruments=value,
                fields=[
                    "TR.FundParentType",
                    "TR.FundInvestorType", 
                    "TR.FundInvtStyleCode",
                    "TR.FundPortfolioName", 
                    "TR.FundTotalEquityAssets", 
                    "TR.FdAdjSharesHeldValue(SortOrder=Descending)",
                    "TR.FdAdjPctOfShrsOutHeld", 
                    "TR.FundPctPortfolio", 
                    "TR.FundAddrCountry", 
                    "TR.FdAdjSharesHeldValue.date"
                ],
                parameters={'EndNum': '1000', "SDate": sdate_for_year, "Curn": "EUR", "Scale": 6}
            )
            logging.info(f"Successfully fetched data for {value} on {sdate_for_year}.")
            return df_tmp
        except (Timeout, HTTPError, RequestException) as err:
            attempts += 1
            logging.warning(f"{err.__class__.__name__} occurred: {err} - Retrying... Attempt {attempts}/{max_retries}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            break
    logging.error(f"Failed to fetch data for {value} after {max_retries} attempts.")
    return None

##############################################################
# Main Execution
##############################################################

def main():
    # Load RIC list
    ric_list = load_ric_list(windows_path)
    
    # Split RIC list into 10 chunks
    lists = split_list_into_chunks(ric_list, num_chunks=10)
    
    # Read date tracking
    start_date, end_date = read_date_tracking(date_tracking_path)
    
    # Generate first days
    first_days = get_first_days(start_date, end_date)
    
    # Define column names
    col_names = [
        "stock_RIC",
        "fund_type_parent",
        "fund_type",
        "fund_investment_type",
        "fund_name",
        "market_cap_fund",
        "stock_value_held",
        "percent_of_traded_shares",
        "percent_of_fund_holdings",
        "country",
        "filing_date",
        "date"
    ]
    
    # Initialize output file
    initialize_output_file(output_file_path, col_names)
    
    # Iterate over each date
    for sdate_for_year in first_days:
        logging.info(f"Starting data retrieval for {sdate_for_year}")
        print(f"Starting data retrieval for {sdate_for_year}")
        
        aggregated_df = pd.DataFrame()
        
        for key, value in lists.items():
            logging.info(f"Extracting data from {key} with {len(value)} RICs.")
            print(f"Currently extracting data from {key}")
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                df_tmp = fetch_data(value, sdate_for_year)
            
            check_conditions(df_tmp)
            if stop_now:
                logging.critical("Stopping execution due to critical error.")
                print(stop_message)
                sys.exit(stop_message)
            
            if df_tmp is not None and not df_tmp.empty:
                df_tmp['date'] = sdate_for_year
                aggregated_df = pd.concat([aggregated_df, df_tmp], ignore_index=True)
            else:
                logging.warning(f"No data returned for {key} on {sdate_for_year}.")
        
        if not aggregated_df.empty:
            aggregated_df.columns = col_names
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=FutureWarning)
                    aggregated_df.to_csv(output_file_path, mode='a', header=False, index=False)
                logging.info(f"Successfully appended data for {sdate_for_year} to {output_file_path}.")
                print(f"-----------------\nSuccessful retrieval till {sdate_for_year}\n-----------------")
            except Exception as e:
                logging.error(f"Error writing to output file: {e}")
        else:
            logging.warning(f"No data aggregated for {sdate_for_year}.")
        
        # Optionally update the date_tracking file to mark progress
        # update_csv(date_tracking_path, new_value=sdate_for_year)

    logging.info("Data retrieval process completed successfully.")

if __name__ == "__main__":
    main()
