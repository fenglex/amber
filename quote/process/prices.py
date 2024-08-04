import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from quote.common.base import SdkBuilder


class Prices:

    def __init__(self):
        self.pro = SdkBuilder().tushare_pro

    def stock_price(self, sd, ed):
        data = []
        cur_date = datetime.strptime(sd, "%Y%m%d")
        while True:

            if int(cur_date.strftime("%Y%m%d")) > int(ed):
                break
            retry = 0
            while retry < 3:
                try:
                    logger.info(f"Getting stock prices for {cur_date.strftime('%Y%m%d')}")
                    df = self.pro.daily(trade_date=cur_date.strftime("%Y%m%d"))
                    if len(df) > 0:
                        data.append(df)
                    break
                except Exception as e:
                    logger.error(e)
                    retry = retry + 1
            cur_date = cur_date + timedelta(days=1)
        df = pd.concat(data)
        df.to_parquet("stock_price.parquet")
        logger.info(f"Stock price data saved to parquet file")
        return df
