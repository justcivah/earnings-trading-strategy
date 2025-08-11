import logging
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def setup_logging():
    """Setup logging system"""
    
    # Prendi il livello dal .env
    log_level = os.getenv("LOG_LEVEL").upper()
    
    # Converti string in livello logging
    level_mapping = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    log_level_num = level_mapping.get(log_level, logging.INFO)
    
    # Crea cartella logs se non esiste
    os.makedirs("logs", exist_ok=True)
    
    # Formato per i log
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level_num)
    
    # Console handler (sempre attivo)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level_num)
    console_handler.setFormatter(formatter)
    
    # File handler per tutti i log
    timestamp = datetime.now().strftime("%Y%m%d")
    file_handler = logging.FileHandler(f"logs/logs_{timestamp}.log", mode="w")
    file_handler.setLevel(logging.DEBUG)  # File cattura tutto
    file_handler.setFormatter(formatter)
    
    # File handler solo per errori
    error_handler = logging.FileHandler(f"logs/errors_{timestamp}.log", mode="w")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Aggiungi handlers
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)
    
    # Configurazioni specifiche per librerie esterne
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)
    logging.getLogger("trafilatura").setLevel(logging.WARNING)
    
    return root_logger

def get_logger(name):
    """Create a more specific logger"""
    return logging.getLogger(name)