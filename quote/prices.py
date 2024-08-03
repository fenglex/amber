import pandas as pd
from tools import to_db


class Prices:

    def __init__(self, pro_api):
        self.pro = pro_api

    # @to_db("tb_stock_eod", update_type='upsert', truncate=True)
    def stock_price(self, stock_code, start_day, end_day):
        data = []
        for code in stock_code:
            df = self.pro.daily(ts_code=stock_code, start_date=start_day, end_date=end_day)
            data.append(df)
        df = pd.concat(data)
        return df
