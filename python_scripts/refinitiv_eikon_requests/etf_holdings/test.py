import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

df = pd.DataFrame({
    'RIC': ['ASML.AS', 'NESN.S', 'SCMN.S', 'ASLO.PA'],
    'name': ["ASML HOLDING NV", "NESTLE SA", "SWISSCOM AG", "ALSOM SA"]
})


sdate_for_year = "2018-01-01" #licence from UZH: 2017-01-01

df, e =ek.get_data(instruments = 'ASML.AS',
                   fields = ['TR.FundAdjShrsHeld.InvestorPermID', 'TR.FundPortfolioName', 'TR.FundAdjShrsHeld', 'TR.FundHoldingsDate'],
                   parameters = {'EndNum':'300', "SDate": sdate_for_year})
print(df)