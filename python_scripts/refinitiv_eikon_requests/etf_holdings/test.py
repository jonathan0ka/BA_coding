import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

df = pd.DataFrame({
    'RIC': ['ASML.AS', 'NESN.S', 'SCMN.S', 'ASLO.PA'],
    'name': ["ASML HOLDING NV", "NESTLE SA", "SWISSCOM AG", "ALSOM SA"]
})


sdate_for_year = "2024-01-01" #licence from UZH: 2017-01-01

df, e =ek.get_data(instruments = 'NESN.S',
                   fields = ["TR.FundInvestorType(TheInvestorType=404)", "TR.FundPortfolioName", 
                             "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue(SortOrder=Descending)", 
                             "TR.FundAddrCountry"],
                   parameters = {'EndNum':'10', "SDate": sdate_for_year, "Curn":"EUR", "Scale":6})


df = df.sort_values("Fund Value Held (Adjusted)", ascending = False)
df.columns = ["stock_RIC", "fund_type", "fund_name", "stock_value_held", "market_cap_fund", "country"]

print(df)
# "TR.FundParentType", "TR.FundInvestorType", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue", "TR.FundAddrCountry",TR.FundAdjShrsHeld, TR.FdAdjSharesHeldValue
