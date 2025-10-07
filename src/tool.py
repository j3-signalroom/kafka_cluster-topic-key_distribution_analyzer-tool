import os
from dotenv import load_dotenv
import logging
from collections import defaultdict
from confluent_kafka import Producer

from utilities import setup_logging
from src.key_distribution_tester import KeyDistributionTester
from confluent_credentials import (fetch_confluent_cloud_credential_via_env_file,
                                   fetch_kafka_credentials_via_env_file,
                                   fetch_kafka_credentials_via_confluent_cloud_api_key)
from constants import (DEFAULT_CHARACTER_REPEAT,
                       DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS,
                       DEFAULT_KAFKA_TOPIC_NAME,
                       DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                       DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                       DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                       DEFAULT_KAFKA_TOPIC_RECORD_COUNT,
                       DEFAULT_USE_AWS_SECRETS_MANAGER)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


def main():
    """Main tool entry point."""

    # Load environment variables from .env file
    load_dotenv()
 
    # Fetch environment variables non-credential configuration settings
    try:
        use_confluent_cloud_api_key_to_fetch_resource_credentials = os.getenv("USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS", DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS) == "True"
        environment_filter = os.getenv("ENVIRONMENT_FILTER")
        kafka_cluster_filter = os.getenv("KAFKA_CLUSTER_FILTER")
        principal_id = os.getenv("PRINCIPAL_ID")
        use_aws_secrets_manager = os.getenv("USE_AWS_SECRETS_MANAGER", DEFAULT_USE_AWS_SECRETS_MANAGER) == "True"
    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return
    
    # Fetch Confluent Cloud credentials from environment variable or AWS Secrets Manager
    metrics_config = fetch_confluent_cloud_credential_via_env_file(use_aws_secrets_manager)
    if not metrics_config:
        return
    
    # Fetch Kafka credentials
    if use_confluent_cloud_api_key_to_fetch_resource_credentials:
        # Read the Kafka Cluster credentials using Confluent Cloud API key
        kafka_credentials = fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id, 
                                                                                metrics_config, 
                                                                                environment_filter, 
                                                                                kafka_cluster_filter)
    else:
        # Read the Kafka Cluster credentials from the environment variable or AWS Secrets Manager
        kafka_credentials = fetch_kafka_credentials_via_env_file(use_aws_secrets_manager)

    if not kafka_credentials:
        return
    
    # Initialize tester
    tester = KeyDistributionTester(kafka_cluster_id=kafka_credentials[0]['kafka_cluster_id'],
                                   bootstrap_server_uri=kafka_credentials[0]['bootstrap_server_uri'],
                                   kafka_api_key=kafka_credentials[0]['kafka_api_key'],
                                   kafka_api_secret=kafka_credentials[0]['kafka_api_secret'])

    # Run comprehensive test
    results = tester.run_comprehensive_test(topic_name=DEFAULT_KAFKA_TOPIC_NAME,
                                            partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                                            replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                            data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                            record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT)

    logging.info("Comprehensive test results: %s", results)

    # Optional: Test with different key patterns
    logging.info("="*DEFAULT_CHARACTER_REPEAT)
    logging.info("Testing with skewed key distribution...")

    # Test skewed distribution
    producer = Producer({
        'bootstrap.servers': kafka_credentials[0]['bootstrap_server_uri'],
        'security.protocol': "SASL_SSL",
        'sasl.mechanism': "PLAIN",
        'sasl.username': kafka_credentials[0]['kafka_api_key'],
        'sasl.password': kafka_credentials[0]['kafka_api_secret'],
        'acks': 'all',
        'retries': 5,
        'linger.ms': 10,
        'batch.size': 16384,
        'buffer.memory': 33554432,
    })

    def delivery_report(error_message, record):
        if error_message is not None:
            logging.error('Message delivery failed: %s', error_message)
        else:
            logging.info('Message delivered to %s [%d]', record.topic(), record.partition())
    
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
    tester.visualize_distribution(skewed_counts, "Skewed Distribution Example")


# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()