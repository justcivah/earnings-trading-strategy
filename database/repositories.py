from sqlalchemy import and_
from database.models import Company, StockPrice, EarningsDate, NewsArticle
from database.connection import db_transaction
from typing import List, Optional, Dict
from datetime import datetime
from shared.utils.logging_utils import get_logger

logger = get_logger(__name__)

class CompanyRepository:
    @staticmethod
    def save_company(symbol: str, name: str, market_cap: int = None, sector: str = None):
        """Save or update a company"""
        with db_transaction() as session:
            company = session.query(Company).filter(Company.symbol == symbol).first()
            
            if company:
                # Update existing
                company.name = name
                company.market_cap = market_cap
                company.sector = sector
            else:
                # Create new
                company = Company(
                    symbol=symbol,
                    name=name,
                    market_cap=market_cap,
                    sector=sector
                )
                session.add(company)
                
            logger.debug(f"Saved company: {symbol} - {name}")

    @staticmethod
    def get_all_symbols() -> List[str]:
        """Get all company symbols"""
        with db_transaction() as session:
            symbols = session.query(EarningsDate.symbol).all()
            return [s[0] for s in symbols]

    @staticmethod
    def get_companies_by_market_cap(min_cap: int, max_cap: int) -> List[Company]:
        """Get companies within market cap range"""
        with db_transaction() as session:
            return session.query(Company).filter(
                and_(
                    Company.market_cap >= min_cap,
                    Company.market_cap <= max_cap
                )
            ).all()

class StockPriceRepository:
    @staticmethod
    def save_stock_prices(stock_data: List[Dict]):
        """Batch save stock prices"""
        with db_transaction() as session:
            for data in stock_data:
                stock_price = StockPrice(
                    symbol=data["symbol"],
                    date=data["date"],
                    open=data["open"],
                    high=data["high"],
                    low=data["low"],
                    close=data["close"],
                    volume=data["volume"]
                )
                session.add(stock_price)
            
            logger.info(f"Saved {len(stock_data)} stock price records")

    @staticmethod
    def get_prices_for_symbol(symbol: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Get stock prices for a symbol in date range"""
        with db_transaction() as session:
            return session.query(StockPrice).filter(
                and_(
                    StockPrice.symbol == symbol,
                    StockPrice.date >= start_date,
                    StockPrice.date <= end_date
                )
            ).order_by(StockPrice.date).all()

    @staticmethod
    def get_price_on_date(symbol: str, date: datetime) -> Optional[StockPrice]:
        """Get stock price for a specific date"""
        with db_transaction() as session:
            return session.query(StockPrice).filter(
                and_(
                    StockPrice.symbol == symbol,
                    StockPrice.date == date
                )
            ).first()

class EarningsRepository:
    @staticmethod
    def save_earnings_dates(earnings_data: List[Dict]):
        """Batch save earnings dates"""
        with db_transaction() as session:
            for data in earnings_data:
                earnings = EarningsDate(
                    symbol=data["symbol"],
                    date=data["date"],
                    eps_estimate=data["eps_estimate"],
                    eps_actual=data["eps_actual"],
                    surprise=data["surprise"]
                )
                session.add(earnings)
            
            logger.info(f"Saved {len(earnings_data)} earnings records")

    @staticmethod
    def get_earnings_in_range(start_date: datetime, end_date: datetime) -> List[EarningsDate]:
        """Get all earnings in date range"""
        with db_transaction() as session:
            return session.query(EarningsDate).filter(
                and_(
                    EarningsDate.date >= start_date,
                    EarningsDate.date <= end_date
                )
            ).order_by(EarningsDate.date).all()

    @staticmethod
    def get_earnings_for_date(date: datetime) -> List[EarningsDate]:
        """Get all earnings for a specific date"""
        with db_transaction() as session:
            return session.query(EarningsDate).filter(
                EarningsDate.date == date
            ).all()

class NewsRepository:
    @staticmethod
    def save_articles(articles: List[Dict]):
        """Batch save news articles"""
        with db_transaction() as session:
            for article in articles:
                news = NewsArticle(
                    symbol=article["symbol"],
                    date=article["date"],
                    title=article["title"],
                    content=article.get("content"),
                    source=article.get("source"),
                    url=article.get("url"),
                    sentiment_score=article.get("sentiment_score")
                )
                session.add(news)
            
            logger.info(f"Saved {len(articles)} news articles")

    @staticmethod
    def get_articles_for_symbol_and_period(symbol: str, start_date: datetime, end_date: datetime) -> List[NewsArticle]:
        """Get news articles for symbol in date range"""
        with db_transaction() as session:
            return session.query(NewsArticle).filter(
                and_(
                    NewsArticle.symbol == symbol,
                    NewsArticle.date >= start_date,
                    NewsArticle.date <= end_date
                )
            ).order_by(NewsArticle.date.desc()).all()

    @staticmethod
    def update_sentiment_scores(sentiment_updates: List[Dict]):
        """Update sentiment scores for articles"""
        with db_transaction() as session:
            for update in sentiment_updates:
                article = session.query(NewsArticle).filter(NewsArticle.id == update["id"]).first()
                if article:
                    article.sentiment_score = update["sentiment_score"]
            
            logger.info(f"Updated sentiment for {len(sentiment_updates)} articles")