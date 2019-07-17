


from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import module1_stock_hist_data as m1

# INSTANTIATE CONNECTION TO ALPHA VANTAGE API______________________________________
ts = TimeSeries(key='0NVCHF5MD13R11PG', output_format='pandas', indexing_type='date')

# Query Individual Stock
m1.get_indv_stock_data('BF.B', ts)
