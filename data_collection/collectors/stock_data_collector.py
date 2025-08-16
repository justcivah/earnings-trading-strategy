import os
import yfinance as yf
from database.repositories import CompanyRepository
from database.repositories import StockPriceRepository
from utils.logging_utils import get_logger
from datetime import timedelta
from datetime import datetime

class StockDataCollector:
    def __init__(self):
        self.logger = get_logger(__name__)

    def collect(self):
        self.logger.info("Starting stock data collection...")

        symbols = CompanyRepository.get_all_symbols()

        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        data_fetch_padding_days = int(os.getenv("DATA_FETCH_PADDING_DAYS"))

        # Convert to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Extend the date range
        extended_start = start_date - timedelta(days=data_fetch_padding_days)
        extended_end = end_date + timedelta(days=data_fetch_padding_days)


        for symbol in symbols:
            self.logger.debug(f"Fetching {symbol} stock data for the period {start_date} - {end_date}...")
            stock_data = yf.download(symbol, start=extended_start, end=extended_end, interval="1d", auto_adjust=True, progress=False)
            self.logger.debug(f"Fetched {symbol} stock data succesfully")

            self.__save_earnings_data(stock_data, symbol)
            
        self.logger.info("Stock data succesfully collected")
        
    def __save_earnings_data(self, stock_data, symbol):
        # Removing multi-stock label
        stock_data.columns = stock_data.columns.droplevel(1)

        # Setting date as clumn instead of index
        stock_data = stock_data.reset_index()

        # Renaming columns
        stock_data = stock_data.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })

        # Changing date format
        stock_data["date"] = stock_data["date"].dt.date

        # Adding symbol
        stock_data["symbol"] = symbol

        if not stock_data.empty:
            StockPriceRepository.save_stock_prices(stock_data.to_dict(orient="records"))
            self.logger.debug(f"Stock data {symbol} succesfully saved in the database")
        else:
            self.logger.warning(f"No stock data found for {symbol}")