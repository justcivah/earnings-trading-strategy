from data_collection.collectors.earnings_collector import EarningsCollector
from data_collection.collectors.company_data_collector import CompanyDataCollector
from data_collection.collectors.stock_data_collector import StockDataCollector
from data_collection.collectors.news_collector import NewsCollector
from data_collection.processors.sentiment_processor import SentimentProcessor
from data_collection.processors.openai_cleanup import OpenAICleanup
from shared.utils.logging_utils import get_logger

class CollectionOrchestrator:
    def __init__(self):
        self.logger = get_logger(__name__)
        
        self.earnings_collector = EarningsCollector()
        self.company_data_collector = CompanyDataCollector()
        self.stock_data_collector = StockDataCollector()
        self.news_collector = NewsCollector()
        self.sentiment_processor = SentimentProcessor()
        self.openai_cleanup = OpenAICleanup()

    def run_full_collection(self):
        """Collects all the data"""

        self.logger.info("=== DATA COLLECTION STARTED ===")
        
        # 1. Get the earning dates
        #self.earnings_collector.collect()
        
        # 2. Get company data from the earnings dates
        #self.company_data_collector.collect()

        # 3. Get stock data for the earnings dates
        #self.stock_data_collector.collect()

        # 4. Get company news
        #self.news_collector.collect()

        # 5. Compute sentiment
        self.sentiment_processor.process()
		# Delete OpenAI batches and remote files
        #self.openai_cleanup.delete()
        
        self.logger.info("=== DATA COLLECTION COMPLETED ===")