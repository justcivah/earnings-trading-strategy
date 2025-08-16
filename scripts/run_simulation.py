from utils.logging_utils import setup_logging, get_logger
from utils.validation_utils import ConfigDataValidator
from simulation.engine.simulation_orchestrator import SimulationOrchestrator

def main():
    setup_logging()
    logger = get_logger(__name__)
    
    try:
        validator = ConfigDataValidator()
        validator.validate_config()
        
        orchestrator = SimulationOrchestrator()
        orchestrator.run_full_simulation()
        
    except Exception as e:
        logger.critical(f"Critical error: {e}")
        raise

if __name__ == "__main__":
    main()