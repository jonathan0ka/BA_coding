import pandas as pd
import eikon as ek
import warnings
import datetime

ek.set_app_key('4b3a2041ad65478b91d46404ba35a4f4d2413f6c')

sdate_for_year = "2006-01-01"

    # Get the constituents of the index
    df, err = ek.get_data(
        instruments=['.STOXX'],
        fields=['TR.IndexConstituentRIC', 'TR.IndexConstituentName'],
        parameters={'SDate': sdate_for_year}
    )

print(df)