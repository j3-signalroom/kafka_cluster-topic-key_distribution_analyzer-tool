from dotenv import load_dotenv
import logging
from collections import defaultdict
from confluent_kafka import Producer

from utilities import setup_logging
from src.key_distribution_test import KeyDistributionTest
from constants import DEFAULT_CHARACTER_REPEAT


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
    
    # Initialize tester
    key_distribution_test = KeyDistributionTest('localhost:9092')
    
    # Run comprehensive test
    results = key_distribution_test.run_comprehensive_test(
        topic_name='python-key-test',
        partition_count=6,
        record_count=1000
    )
    
    # Optional: Test with different key patterns
    logging.info("="*DEFAULT_CHARACTER_REPEAT)
    logging.info("Testing with skewed key distribution...")

    # Test skewed distribution
    producer = Producer({
        'bootstrap.servers': 'localhost:9092'
    })

    def delivery_report(err, msg):
        if err is not None:
            logging.error('Message delivery failed: %s', err)
        else:
            logging.info('Message delivered to %s [%d]', msg.topic(), msg.partition())

    # Create skewed data (80% of messages go to same key pattern)
    skewed_partition_mapping = defaultdict(list)
    
    for i in range(500):
        # 80% of messages use the same key
        if i < 400:
            key = "hot-key-1"
        else:
            key = f"cold-key-{i}"
        
        future = producer.send('python-key-test', key=key, value={"id": i})
        try:
            record_metadata = future.get(timeout=10)
            skewed_partition_mapping[record_metadata.partition].append(key)
        except Exception as e:
            logging.error("Error: %s", e)
    
    producer.close()
    
    # Analyze skewed distribution
    skewed_counts = {p: len(keys) for p, keys in skewed_partition_mapping.items()}
    key_distribution_test.visualize_distribution(skewed_counts, "Skewed Distribution Example")


# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()