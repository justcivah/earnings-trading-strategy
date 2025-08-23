from dotenv import load_dotenv
from utils.validation_utils import ConfigDataValidator
from parameter_tests.parameter_tester import run_full_test
from utils.logging_utils import setup_logging

def main():
    setup_logging()
    
    validator = ConfigDataValidator()
    validator.validate_config()
    
    run_full_test()

if __name__ == "__main__":
    main()