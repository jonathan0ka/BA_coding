import pandas as pd
import eikon as ek
import warnings
from requests.exceptions import HTTPError, RequestException
import csv

ek.set_app_key('80631b9534434526bb7b73ad26db914d4c2d9769')

# Global stop variables
stop_now = False
stop_message = "Critical error: Connection Lost"

def check_conditions(df_tmp):
    global stop_now
    if df_tmp is None:
        stop_now = True
        print(stop_message)

def split_into_lists(ric_df, num_lists=10):
    ric_list = pd.unique(ric_df['stock_RIC']).tolist()
    print(f"Total unique RICs: {len(ric_list)}")
    return {f'list_{i+1}': ric_list[i::num_lists] for i in range(num_lists)}

def read_dates(csv_path):
    with open(csv_path, mode='r', newline='') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header
        return next(csv_reader)  # Read the first row of dates

def get_first_days(start_date, end_date):
    return pd.date_range(start=start_date, end=end_date, freq='MS').strftime('%Y-%m-%d').tolist()

def fetch_data(value, sdate_for_year, max_retries=2):
    attempts = 0
    while attempts < max_retries:
        try:
            return ek.get_data(
                instruments=value,
                fields=[
                    "TR.FundParentType", "TR.FundInvestorType", "TR.FundInvtStyleCode",
                    "TR.FundPortfolioName", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue(SortOrder=Descending)",
                    "TR.FdAdjPctOfShrsOutHeld", "TR.FundPctPortfolio", "TR.FundAddrCountry", "TR.FdAdjSharesHeldValue.date"
                ],
                parameters={'EndNum': '1000', "SDate": sdate_for_year, "Curn": "EUR", "Scale": 6}
            )
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
    file_path_date_tracking = 'C:\\Users\\Shadow\\OneDrive\\BA_Thesis\\BA_coding\\datasets\\eikon_data\\index_constituents_data\\date_tracking.csv'
    start_date, end_date = read_dates(file_path_date_tracking)
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
            df = pd.concat([df, df_tmp], ignore_index=True)

        df.columns = col_names
        df.to_csv(file_path, mode='a', header=False, index=False)
        print(f"-----------------\nSuccessful retrieval till {sdate_for_year}\n-----------------")

if __name__ == '__main__':
    main()
