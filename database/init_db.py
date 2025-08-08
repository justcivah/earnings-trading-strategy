import os
from sqlalchemy import create_engine, text
from database.models import Base
from database.connection import DATABASE_URL
from shared.utils.logging_utils import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

def init_database():
    """Initialize SQLite database with all tables and indexes"""
    logger.info("Initializing database...")

    # Ensure the folder exists
    db_path = DATABASE_URL.replace("sqlite:///", "")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

    try:
        logger.warning("Dropping all existing tables...")
        Base.metadata.drop_all(engine)
        logger.info("Creating all tables...")
        Base.metadata.create_all(engine)

        logger.info("Creating indexes...")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_prices (symbol, date)"
                )
            )
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_stock_date ON stock_prices (date)")
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_earnings_symbol_date ON earnings_dates (symbol, date)"
                )
            )
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_earnings_date ON earnings_dates (date)")
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_news_symbol_date ON news_articles (symbol, date)"
                )
            )
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_news_date ON news_articles (date)")
            )
            conn.commit()

        logger.info("Database initialized successfully!")

    except Exception as e:
        logger.critical(f"Error initializing database: {e}")

if __name__ == "__main__":
    init_database()