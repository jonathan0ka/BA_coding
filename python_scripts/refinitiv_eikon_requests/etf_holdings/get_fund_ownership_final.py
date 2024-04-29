import pandas as pd
import eikon as ek
import warnings
from requests.exceptions import HTTPError, RequestException
import csv

ek.set_app_key('80631b9534434526bb7b73ad26db914d4c2d9769')

# Global stop variables
stop_now = False
stop_message = "Critical error: Connection Lost"
start_date = "2019-01-01"
end_date = "2024-01-01"

def check_conditions(df_tmp):
    global stop_now
    if df_tmp is None:
        stop_now = True
        print(stop_message) 

def split_into_lists(ric_df, num_lists=10):
    ric_list = pd.unique(ric_df['stock_RIC']).tolist()
    print(f"Total unique RICs: {len(ric_list)}")
    return {f'list_{i+1}': ric_list[i::num_lists] for i in range(num_lists)}

def get_first_days(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y-%m-%d').tolist()

def split_dates_into_lists(num_lists=10):
    
    print(f"Total unique RICs: {len(ric_list)}")
    return {f'list_{i+1}': ric_list[i::num_lists] for i in range(num_lists)}
    







def fetch_data(value, sdate_for_year, max_retries=2):
    attempts = 0
    while attempts < max_retries:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                    df_tmp, e = ek.get_data(
                        instruments=value,
                        fields=[
                            "TR.FundParentType", "TR.FundInvestorType", "TR.FundInvtStyleCode",
                            "TR.FundPortfolioName", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue(SortOrder=Descending)",
                            "TR.FdAdjPctOfShrsOutHeld", "TR.FundPctPortfolio", "TR.FundAddrCountry", "TR.FdAdjSharesHeldValue.date"
                        ],
                parameters={'EndNum': '1000', "SDate": sdate_for_year, "Curn": "EUR", "Scale": 6}
            )
            return df_tmp
        except (HTTPError, RequestException) as e:
            print(f"{e.__class__.__name__} occurred: {e} - Retrying... Attempt {attempts + 1}/{max_retries}")
            attempts += 1
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
    return None

def main():
    windows_path = 'C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\formated_constituents_stoxx_europe_600.csv'
    ric_df = pd.read_csv(windows_path)
    lists = split_into_lists(ric_df)
    first_days = get_first_days(start_date, end_date)

    file_path = "C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\fund_holdings_data\\etf_holdings_600_stocks_test.csv"
    col_names = ["stock_RIC", "fund_type_parent", "fund_type", "fund_investment_type", "fund_name",
                 "market_cap_fund", "stock_value_held", "percent_of_traded_shares", "percent_of_fund_holdings",
                 "country", "filing_date", "date"]

    for sdate_for_year in first_days:
        if stop_now:
            print(stop_message)
            break

        print(f"Starting with data retrieval for {sdate_for_year}")
        df = pd.DataFrame()

        for key, value in lists.items():
            print(f"Currently extracting data from {key}")
            df_tmp = fetch_data(value, sdate_for_year)

            if df_tmp is None:
                print("Failed to retrieve data.")
                break

            df_tmp['date'] = sdate_for_year
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                df = pd.concat([df, df_tmp], ignore_index=True)

        if df_tmp is not None:
            df.columns = col_names
            df.to_csv(file_path, mode='a', header=False, index=False)
            print(f"-----------------\nSuccessful retrieval till {sdate_for_year}\n-----------------")
            

if __name__ == '__main__':
    main()


# def read_dates(csv_path):
#     with open(csv_path, mode='r', newline='') as file:
#         csv_reader = csv.reader(file)
#         next(csv_reader)  # Skip header
#         return next(csv_reader)  # Read the first row of dates

# def update_start_date(csv_path, new_start_date):
#     try:
#         with open(csv_path, mode='r', newline='') as file:
#             csv_reader = csv.reader(file)
#             data = list(csv_reader)

#         if len(data) > 1:  # Ensure there is at least one data row
#             data[1][0] = new_start_date  # Update start_date

#         with open(csv_path, mode='w', newline='') as file:
#             csv_writer = csv.writer(file)
#             csv_writer.writerows(data)
#         print("Start date has been updated in the CSV file")
#     except Exception as e:
#         print(f"An error occurred while updating the CSV file: {e}")


