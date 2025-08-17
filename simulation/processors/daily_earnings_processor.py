import os
from utils.logging_utils import get_logger
from database.repositories import EarningsRepository
from database.repositories import CompanyRepository
from simulation.indexes.rsi import RSI
from simulation.indexes.volume import Volume
from simulation.indexes.sentiment import Sentiment

class DailyEarningsProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)
        
        self.rsi = RSI()
        self.volume = Volume()
        self.sentiment = Sentiment()

    def compute(self, date):
        self.logger.info(f"Starting processing earnings for {date}...")

        min_market_cap = int(os.getenv("MIN_MARKET_CAP"))
        max_market_cap = int(os.getenv("MAX_MARKET_CAP"))

        all_earnings = EarningsRepository.get_earnings_for_date(date)
        filtered_earnings = [] 
        operations = {}

        for earnings in all_earnings:
            company = CompanyRepository.get_company(earnings["symbol"])

            # Filter companies
            if company["market_cap"] is not None and min_market_cap <= company["market_cap"] <= max_market_cap:
                filtered_earnings.append(earnings)
            
            rsi = self.rsi.compute_rsi(earnings["symbol"], date)
            volume_trend = self.volume.compute_volume_trend(earnings["symbol"], date)
            volume_average = self.volume.compute_volume_average(earnings["symbol"], date)
            volume_accumulation_distribution = self.volume.compute_volume_accumulation_distribution(earnings["symbol"], date)
            sentiment = self.sentiment.compute(earnings["symbol"], date)

            print(f"RSI: {rsi}")
            print(f"Volume trend: {volume_trend}")
            print(f"Volume average: {volume_average}")
            print(f"Volume accumulation/distribution: {volume_accumulation_distribution}")
            print(f"Sentiment: {sentiment}")
                
        if len(all_earnings) == 0 or len(filtered_earnings) == 0:
            self.logger.info("No valid earnings for that date")
            
        self.logger.info("Earnings succesfully processed")

        return filtered_earnings