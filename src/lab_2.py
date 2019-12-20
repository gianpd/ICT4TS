"""ICT4TS LAB 2: build a model able to predict future rentals in time by exploiting the historically data i.e gives the
number of rental in the past predict the number of rental in the future.
First step: check if and under what conditions the time series are stationary
Second step: compute the ACF and/or PACF for determining the grade of the lag
Third Step: build the right ARIMA model for the data.
Forty Step: test the model.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from connection import Connection
from pipeline_ict4ts import pip_uot, pip_uotF, pip_hmt, pip_d
from timeseries import test_stationarity, test_residual, get_d


client = Connection()
db = client.db

start = datetime.timestamp(datetime(2016, 12, 13))
end = datetime.timestamp(datetime(2018, 1, 31))


to_b_uotF = list(db['PermanentBookings'].aggregate(pip_uotF(start_timestamp=start, end_timestamp=end,
                                                                 objective=False)))


eto_b_uotF = list(db['enjoy_PermanentBookings'].aggregate(pip_uotF(start_timestamp=start, end_timestamp=end,
                                                                        objective=False)))


po_b_uotF_ = list(db['PermanentBookings'].aggregate(pip_uotF(city='Portland', start_timestamp=start,
                                                                 end_timestamp=end,
                                                                objective=False)))

df_b_t = pd.DataFrame(to_b_uotF)
df_b_te = pd.DataFrame(eto_b_uotF)
df_b_po = pd.DataFrame(po_b_uotF_)

df_b_t.to_csv('t_r_ts.csv')
df_b_te.to_csv('te_r_ts.csv')
df_b_po.to_csv('po_r_ts.csv')

date_t = df_b_t['date'].astype('O')
date_te = df_b_te['date'].astype('O')
date_po = df_b_po['date'].astype('O')

total_t = df_b_t['total']
total_te = df_b_te['total']
total_po = df_b_po['total']

test_stationarity(date_t, total_t, model='Turin')
test_stationarity(date_te, total_te, model='Turin_enjoy')
test_stationarity(date_po, total_po, model='Portland')


















