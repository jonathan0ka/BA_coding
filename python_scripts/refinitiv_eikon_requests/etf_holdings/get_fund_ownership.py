import pandas as pd
import eikon as ek
import warnings
from datetime import datetime
from pandas import date_range
from requests.exceptions import HTTPError, RequestException, Timeout
import csv

ek.set_app_key('940ef42d301045f88d57ab197ddd79e4e45f93f0')

##############################################################
# global stop
##############################################################
stop_now = False
stop_message = "Critical error: Conncetion Lost"

def check_conditions(df_tmp):
    global stop_now
    # If the dataframe from the API call is None, set stop_now to True
    if df_tmp is None:
        stop_now = True
        print(stop_message)


##############################################################
# split stock_RIC into 10 equally large lists
##############################################################

# Import the CSV file containing the stock RICs
mac_path = "/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/index_constituents_data/formated_constituents_stoxx_europe_600.csv"
windows_path = 'C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv'
ric_df = pd.read_csv(windows_path)


# Extract the RICs into a list
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    ric_list = list(pd.unique(ric_df['stock_RIC'].tolist()))
print(len(ric_list))

# Number of desired lists
num_lists = 10

# Calculate the size of each list (assuming the list can be evenly divided)
size_each_list = len(ric_list) // num_lists

# Create a dictionary to hold the lists
lists = {}

# Split the list into 10 equally sized lists
for i in range(num_lists):
    start_index = i * size_each_list
    # Adjust the end index to avoid index out of range
    end_index = start_index + size_each_list if i < num_lists - 1 else len(ric_list)
    lists[f'list_{i + 1}'] = ric_list[start_index:end_index]

########################################################################
# dates
########################################################################
def get_first_days(start_date, end_date):
    # Create a date range from start date to end date with monthly frequency, starting at the first day of each month
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    # Format dates as strings and return as a list
    return [date.strftime('%Y-%m-%d') for date in date_range]

########################################################################
# dates storage
########################################################################
file_path_date_tracking = 'C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\date_tracking.csv'

# with open(file_path_date_tracking, mode='w', newline='') as file:
#     csv_writer = csv.writer(file)
#     headers = ["start_date", "end_date"]
#     csv_writer.writerow(headers)
#     csv_writer.writerow(['2014-07-01', '2024-01-01'])
####################### ####################### ####################### 
    
def update_csv(file_path, new_value, row_index=0, column_index=0):
    data = []

    # Modify the value at the specified row and column
    data[row_index][column_index] = new_value

    # Write the data back to the CSV file
    with open(file_path, mode='w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data)
    print("date_tracking.csv has been successfully updated")
    
with open(file_path_date_tracking, mode='r', newline='') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)
    row = next(csv_reader)
    start_date = row[0]
    end_date = row[1]

first_days = get_first_days(start_date, end_date)

########################################################################
# initiate data frame
########################################################################
# empty DataFrame to aggregate the results
col_names = ["stock_RIC",
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
               "date"]                                                                                                                                                   

#aggregated_df = pd.DataFrame(columns = col_names)

file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\fund_holdings_data\\etf_holdings_600_stocks_2016_12.csv"
#aggregated_df.to_csv(file_path, index=False)

########################################################################
# api call function
########################################################################
def fetch_data(value, sdate_for_year, max_retries=2):
    attempts = 0
    while attempts < max_retries:
        try:
            df_tmp, e = ek.get_data(
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
            return df_tmp  # Return data frame if the call is successful
        
        except Timeout as timeout_err:
            print(f"Timeout occurred: {timeout_err} - Retrying... Attempt {attempts + 1}/{max_retries}")
            attempts += 1
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} - Retrying... Attempt {attempts + 1}/{max_retries}")
            attempts += 1
        except RequestException as req_err:
            print(f"Request error occurred: {req_err} - Retrying... Attempt {attempts + 1}/{max_retries}")
            attempts += 1
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break  # Break on non-HTTP, non-request errors to avoid infinite loop
    return None  # Return None if all retries fail

########################################################################
# api call 
########################################################################
for sdate_for_year in first_days:
    #update_csv(file_path, sdate_for_year)
    print(f"Starting with data retrieval for {sdate_for_year}")

    df = pd.DataFrame()

    for key, value in lists.items():
        print(f"Currently extracting data from {key}")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning)

            df_tmp = fetch_data(value, sdate_for_year)

            # Check if we should stop after this fetch
            check_conditions(df_tmp)  
            if stop_now:
                break

            df_tmp['date'] = sdate_for_year
            df = pd.concat([df, df_tmp], ignore_index=True)

    if stop_now:
        print(stop_message)
        break

    df.columns = col_names
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
    
        # Append DataFrame to existing CSV file
        df.to_csv(file_path, mode='a', header=False, index=False)
        print(f"-----------------\nSuccessful retrieval till {sdate_for_year}\n-----------------")