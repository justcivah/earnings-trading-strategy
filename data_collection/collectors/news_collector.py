import os
import time
import pandas as pd
import finnhub
import trafilatura
import requests
from datetime import datetime, timedelta
from database.repositories import EarningsRepository, CompanyRepository, NewsRepository
from shared.utils.logging_utils import get_logger

class NewsCollector:
    def __init__(self):
        self.logger = get_logger(__name__)

    def collect(self):
        self.logger.info("Starting news collection...")

        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        
        max_news_0_1_days = int(os.getenv("MAX_NEWS_0_1_DAYS"))
        max_news_2_4_days = int(os.getenv("MAX_NEWS_2_4_DAYS"))
        max_news_5_7_days = int(os.getenv("MAX_NEWS_5_7_DAYS"))

        for datetime in pd.date_range(start=start_date, end=end_date):
            earnings = EarningsRepository.get_earnings_for_date(datetime.date())
            
            for earning in earnings:
                company = CompanyRepository.get_company(earning["symbol"])
                
                self.logger.debug(f"{earning['date']} {earning['symbol']} {company['name']}")
                
                # Collect news for different periods
                self.__collect_news_for_periods(earning, company, max_news_0_1_days, max_news_2_4_days, max_news_5_7_days)
                
                time.sleep(int(os.getenv("SCRAPING_DELAY")))

        self.logger.info("News successfully collected")

    def __collect_news_for_periods(self, earning, company, news_0_1, max_news_2_4, max_news_5_7):
        """Collect news for different time periods before earnings"""
        earning_date = earning["date"]
        
        # Period 1: 0-1 days before (day before earnings)
        start_date = earning_date - timedelta(days=1)
        end_date = earning_date - timedelta(days=1)
        self.__collect_news_for_period(earning["symbol"], start_date, end_date, news_0_1, "0-1 days")
        
        # Period 2: 2-4 days before
        start_date = earning_date - timedelta(days=4)
        end_date = earning_date - timedelta(days=2)
        self.__collect_news_for_period(earning["symbol"], start_date, end_date, max_news_2_4, "2-4 days")
        
        # Period 3: 5-7 days before
        start_date = earning_date - timedelta(days=7)
        end_date = earning_date - timedelta(days=5)
        self.__collect_news_for_period(earning["symbol"], start_date, end_date, max_news_5_7, "5-7 days")

    def __collect_news_for_period(self, symbol, start_date, end_date, max_articles, period_name):
        """Collect news for a specific period and save all articles at once"""
        self.logger.debug(f"Collecting {max_articles} articles for {symbol} ({period_name}): {start_date} to {end_date}")
        
        finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))
        articles_to_save = []  # List to collect articles for this period

        try:
            articles = finnhub_client.company_news(symbol, _from=start_date, to=end_date)
            yahoo_articles = [a for a in articles if a.get("source") == "Yahoo"]
            yahoo_articles = yahoo_articles[:max_articles]

            for article in yahoo_articles:
                url = self.__get_redirect_url(article.get("url"))

                try:
                    downloaded = trafilatura.fetch_url(url)
                    if downloaded:
                        content = trafilatura.extract(downloaded)
                        content = self.__remove_formatting(content)
                    else:
                        content = None

                    if not content:
                        self.logger.warning(f"Could not extract content from {url}")
                        continue

                    date = datetime.fromtimestamp(article.get("datetime")).date()

                    # Add article to the list instead of saving immediately
                    article_data = {
                        "symbol": symbol,
                        "date": date,
                        "headline": article.get("headline"),
                        "content": content,
                        "summary": article.get("summary"),
                        "source": article.get("source"),
                        "url": url,
                        "sentiment_score": None
                    }
                    articles_to_save.append(article_data)

                    self.logger.debug(f"Collected article: {article.get('headline')}")

                except Exception as e:
                    self.logger.error(f"Error extracting article from {url}: {e}")

            # Save all articles for this period at once
            self.__save_articles_batch(articles_to_save, symbol, period_name)
            
            time.sleep(int(os.getenv("SCRAPING_DELAY")))

        except Exception as e:
            self.logger.warning(f"Error while collecting {max_articles} articles for {symbol} ({period_name}): {e}")

    def __save_articles_batch(self, articles, symbol, period_name):
        """Save a batch of articles to database"""
        if not articles:
            self.logger.debug(f"No articles to save for {symbol} ({period_name})")
            return
        
        try:
            NewsRepository.save_articles(articles)
            self.logger.debug(f"Successfully saved {len(articles)} articles for {symbol} ({period_name})")
        except Exception as e:
            self.logger.error(f"Error saving articles for {symbol} ({period_name}): {e}")

    def __remove_formatting(self, text):
        return ' '.join(text.split())
    
    def __get_redirect_url(self, url):
        try:
            response = requests.get(url, allow_redirects=False, timeout=10)

            if 300 <= response.status_code < 400:
                return response.headers.get("Location")
            return None
        
        except requests.RequestException as e:
            print(f"Error fetching URL: {e}")
            return None