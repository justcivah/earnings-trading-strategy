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
    def get_companies_by_market_cap(min_cap: int, max_cap: int) -> List[Dict]:
        """Get companies within market cap range"""
        with db_transaction() as session:
            companies = session.query(Company).filter(
                and_(
                    Company.market_cap >= min_cap,
                    Company.market_cap <= max_cap
                )
            ).all()
            
            return [
                {
                    "symbol": company.symbol,
                    "name": company.name,
                    "market_cap": company.market_cap,
                    "sector": company.sector
                }
                for company in companies
            ]
        
    @staticmethod
    def get_company(symbol: str) -> Optional[Dict]:
        """Get detailed company data by symbol."""
        with db_transaction() as session:
            company = session.query(Company).filter(Company.symbol == symbol).first()
            if company:
                return {
                    "symbol": company.symbol,
                    "name": company.name,
                    "market_cap": company.market_cap,
                    "sector": company.sector
                }
            return None

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
    def get_prices_for_symbol(symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get stock prices for a symbol in date range"""
        with db_transaction() as session:
            prices = session.query(StockPrice).filter(
                and_(
                    StockPrice.symbol == symbol,
                    StockPrice.date >= start_date,
                    StockPrice.date <= end_date
                )
            ).order_by(StockPrice.date).all()
            
            return [
                {
                    "symbol": price.symbol,
                    "date": price.date,
                    "open": price.open,
                    "high": price.high,
                    "low": price.low,
                    "close": price.close,
                    "volume": price.volume
                }
                for price in prices
            ]

    @staticmethod
    def get_price_on_date(symbol: str, date: datetime) -> Optional[Dict]:
        """Get stock price for a specific date"""
        with db_transaction() as session:
            price = session.query(StockPrice).filter(
                and_(
                    StockPrice.symbol == symbol,
                    StockPrice.date == date
                )
            ).first()
            
            if price:
                return {
                    "symbol": price.symbol,
                    "date": price.date,
                    "open": price.open,
                    "high": price.high,
                    "low": price.low,
                    "close": price.close,
                    "volume": price.volume
                }
            return None

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
    def get_earnings_in_range(start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get all earnings in date range"""
        with db_transaction() as session:
            earnings = session.query(EarningsDate).filter(
                and_(
                    EarningsDate.date >= start_date,
                    EarningsDate.date <= end_date
                )
            ).order_by(EarningsDate.date).all()
            
            return [
                {
                    "symbol": earning.symbol,
                    "date": earning.date,
                    "eps_estimate": earning.eps_estimate,
                    "eps_actual": earning.eps_actual,
                    "surprise": earning.surprise
                }
                for earning in earnings
            ]

    @staticmethod
    def get_earnings_for_date(date: datetime) -> List[Dict]:
        """Get all earnings for a specific date"""
        with db_transaction() as session:
            earnings = session.query(EarningsDate).filter(
                EarningsDate.date == date
            ).all()
            
            return [
                {
                    "symbol": earning.symbol,
                    "date": earning.date,
                    "eps_estimate": earning.eps_estimate,
                    "eps_actual": earning.eps_actual,
                    "surprise": earning.surprise
                }
                for earning in earnings
            ]

class NewsRepository:
    @staticmethod
    def save_articles(articles: List[Dict]):
        """Batch save news articles"""
        with db_transaction() as session:
            for article in articles:
                news = NewsArticle(
                    symbol=article["symbol"],
                    date=article["date"],
                    headline=article["headline"],
                    summary=article.get("summary"),
                    content=article.get("content"),
                    source=article.get("source"),
                    url=article.get("url"),
                    sentiment_score=article.get("sentiment_score"),
                    sentiment_reasoning=article.get("sentiment_reasoning")
                )
                session.add(news)
            
            logger.info(f"Saved {len(articles)} news articles")

    @staticmethod
    def get_articles_for_symbol_and_period(symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get news articles for symbol in date range"""
        with db_transaction() as session:
            articles = session.query(NewsArticle).filter(
                and_(
                    NewsArticle.symbol == symbol,
                    NewsArticle.date >= start_date,
                    NewsArticle.date <= end_date
                )
            ).order_by(NewsArticle.date.desc()).all()
            
            return [
                {
                    "id": article.id,
                    "symbol": article.symbol,
                    "date": article.date,
                    "headline": article.headline,
                    "summary": article.summary,
                    "content": article.content,
                    "source": article.source,
                    "url": article.url,
                    "sentiment_score": article.sentiment_score,
                    "sentiment_reasoning": article.sentiment_reasoning
                }
                for article in articles
            ]
        
    @staticmethod
    def get_articles_for_date(date: datetime) -> List[Dict]:
        """Get all news articles for a specific date"""
        with db_transaction() as session:
            articles = (
                session.query(NewsArticle)
                .filter(NewsArticle.date == date)
                .order_by(NewsArticle.date.desc())
                .all()
            )

            return [
                {
                    "id": article.id,
                    "symbol": article.symbol,
                    "date": article.date,
                    "headline": article.headline,
                    "summary": article.summary,
                    "content": article.content,
                    "source": article.source,
                    "url": article.url,
                    "sentiment_score": article.sentiment_score,
                    "sentiment_reasoning": article.sentiment_reasoning
                }
                for article in articles
            ]

    @staticmethod
    def update_article_sentiment(article_id: int, sentiment_score: float, sentiment_reasoning: str):
        """Update sentiment score for a single article"""
        with db_transaction() as session:
            article = session.query(NewsArticle).filter(NewsArticle.id == article_id).first()
            if article:
                article.sentiment_score = sentiment_score
                article.sentiment_reasoning = sentiment_reasoning
                logger.debug(f"Updated sentiment for article {article_id}")