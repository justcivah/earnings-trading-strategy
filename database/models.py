from sqlalchemy import Column, Integer, String, Date, Float, Text, ForeignKey, BigInteger
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    
    symbol = Column(String(10), primary_key=True)
    name = Column(String(255))
    market_cap = Column(BigInteger)
    sector = Column(String(100))
    
    # Relationships
    stock_prices = relationship("StockPrice", back_populates="company")
    earnings_dates = relationship("EarningsDate", back_populates="company")
    news_articles = relationship("NewsArticle", back_populates="company")

class StockPrice(Base):
    __tablename__ = "stock_prices"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), ForeignKey("companies.symbol"), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    
    # Relationship
    company = relationship("Company", back_populates="stock_prices")

class EarningsDate(Base):
    __tablename__ = "earnings_dates"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), ForeignKey("companies.symbol"), nullable=False)
    date = Column(Date, nullable=False)
    eps_estimate = Column(Float)
    eps_actual = Column(Float)
    surprise = Column(Float)
    
    # Relationship
    company = relationship("Company", back_populates="earnings_dates")

class NewsArticle(Base):
    __tablename__ = "news_articles"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), ForeignKey("companies.symbol"), nullable=False)
    date = Column(Date, nullable=False)
    headline = Column(Text, nullable=False)
    summary = Column(Text, nullable=False)
    content = Column(Text)
    source = Column(String(255))
    url = Column(Text)
    sentiment_score = Column(Float)
    
    # Relationship
    company = relationship("Company", back_populates="news_articles")