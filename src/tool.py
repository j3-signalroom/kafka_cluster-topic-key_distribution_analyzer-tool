from dotenv import load_dotenv

from utilities import setup_logging


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


def main():
    """Main application entry point."""

    # Load environment variables from .env file
    load_dotenv()
        

# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()