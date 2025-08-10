import yfinance as yf
from database.repositories import CompanyRepository
from shared.utils.logging_utils import get_logger

class CompanyDataCollector:
    def __init__(self):
        self.logger = get_logger(__name__)

    def collect(self):
        self.logger.info("Starting company data collection...")

        symbols = CompanyRepository.get_all_symbols()

        for symbol in symbols:
            self.logger.debug(f"Fetching {symbol} data...")
            info = yf.Ticker(symbol).info
            self.logger.debug(f"Fetched {symbol} data succesfully")

            CompanyRepository.save_company(symbol, info.get('longName'), info.get('marketCap'), info.get('sector'))
            self.logger.debug(f"Company {symbol} succesfully saved in the database")
        
        self.logger.info("Company data succesfully collected")