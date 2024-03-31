import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

df = pd.DataFrame({
    'RIC': ['ASML.AS', 'NESN.S', 'SCMN.S', 'ASLO.PA'],
    'name': ["ASML HOLDING NV", "NESTLE SA", "SWISSCOM AG", "ALSOM SA"]
})


sdate_for_year = "2010-02-01" #licence from UZH: 2017-01-01

df, e =ek.get_data(instruments = 'ASML.AS',
                   fields = ["TR.FundParentType(TheInvestorType=400)", "TR.FundInvestorType(TheInvestorType=404)", "TR.FundPortfolioName", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue", "TR.FundAddrCountry"],
                   parameters = {'EndNum':'300', "SDate": sdate_for_year})
print(df)


# "TR.FundParentType", "TR.FundInvestorType", "TR.FundTotalEquityAssets", "TR.FdAdjSharesHeldValue", "TR.FundAddrCountry"
