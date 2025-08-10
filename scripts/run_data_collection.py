from shared.utils.logging_utils import setup_logging, get_logger
from shared.utils.validation_utils import ConfigDataValidator
from data_collection.schedulers.collection_orchestrator import CollectionOrchestrator

def main():
    # Prima cosa: setup logging
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        validator = ConfigDataValidator()
        validator.validate_config()
        
        orchestrator = CollectionOrchestrator()
        orchestrator.run_full_collection()
        
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        raise

if __name__ == "__main__":
    main()