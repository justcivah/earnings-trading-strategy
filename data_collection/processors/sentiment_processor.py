import os
import pandas as pd
from shared.utils.logging_utils import get_logger
from database.repositories import NewsRepository

class SentimentProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)

    def process(self):
        self.logger.info("Starting sentiment processing...")

        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")

        for datetime in pd.date_range(start=start_date, end=end_date):
            date = datetime.date()
            self.logger.debug(f"Analyzing news from {date}")
        
            articles = NewsRepository.get_articles_for_date(date)
            for article in articles:
