import os
from datetime import datetime, timedelta
from utils.logging_utils import get_logger
from database.repositories import NewsRepository

class Sentiment:
    def __init__(self):
        self.logger = get_logger(__name__)
          
    def compute(self, symbol, date):
        """Calculate sentiment for all the news of a given timeframe"""

        start_date = os.getenv("START_DATE")
        
        news = NewsRepository.get_articles_for_symbol_and_period(symbol, start_date, date)

        if len(news) == 0:
            self.logger.warning(f"No news are available for {symbol}")
            return 0

        total_sentiment = sum(data["sentiment_score"] for data in news)        
        return total_sentiment / len(news)