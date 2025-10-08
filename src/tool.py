import os
from dotenv import load_dotenv
import logging
from collections import defaultdict

from utilities import setup_logging
from key_distribution_tester import KeyDistributionTester
from key_data_skew_tester import KeyDataSkewTester
from confluent_credentials import (fetch_confluent_cloud_credential_via_env_file,
                                   fetch_kafka_credentials_via_env_file,
                                   fetch_kafka_credentials_via_confluent_cloud_api_key)
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS,
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

    # Create skewed data (80% of messages go to same key pattern)
    skewed_partition_mapping = defaultdict(list)

    def delivery_report(error_message, record):
        try:
            logging.info(f"Message delivered to partition {record.partition()}")
            logging.info(f"Message key: {record.key().decode('utf-8')}")
            skewed_partition_mapping[record.partition()].append(record.key().decode('utf-8'))
        except Exception as e:
            logging.error(f"Error Message, {error_message} in delivery callback: {e}")

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
    cc_credential = fetch_confluent_cloud_credential_via_env_file(use_aws_secrets_manager)
    if not cc_credential:
        return
    
    # Fetch Kafka credentials
    if use_confluent_cloud_api_key_to_fetch_resource_credentials:
        # Read the Kafka Cluster credentials using Confluent Cloud API key
        kafka_credentials = fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id, 
                                                                                cc_credential, 
                                                                                environment_filter, 
                                                                                kafka_cluster_filter)
    else:
        # Read the Kafka Cluster credentials from the environment variable or AWS Secrets Manager
        kafka_credentials = fetch_kafka_credentials_via_env_file(use_aws_secrets_manager)

    if not kafka_credentials:
        return

    # Initialize Key Distribution Tester
    distribution_test = KeyDistributionTester(kafka_cluster_id=kafka_credentials[0]['kafka_cluster_id'],
                                              bootstrap_server_uri=kafka_credentials[0]['bootstrap.servers'],
                                              kafka_api_key=kafka_credentials[0]['sasl.username'],
                                              kafka_api_secret=kafka_credentials[0]['sasl.password'])

    # Run Key Distribution Test
    distribution_results = distribution_test.run_test(topic_name=DEFAULT_KAFKA_TOPIC_NAME,
                                                      partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                                                      replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                      data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                                      record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT)

    logging.info("Key Distribution Test Results: %s", distribution_results)

    # Initialize Key Data Skew Tester
    data_skew_test = KeyDataSkewTester(kafka_cluster_id=kafka_credentials[0]['kafka_cluster_id'],
                                       bootstrap_server_uri=kafka_credentials[0]['bootstrap.servers'],
                                       kafka_api_key=kafka_credentials[0]['sasl.username'],
                                       kafka_api_secret=kafka_credentials[0]['sasl.password'])

    # Run Key Data Skew Test
    data_skew_results = data_skew_test.run_test(topic_name=DEFAULT_KAFKA_TOPIC_NAME,
                                                partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                                                replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                                record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT)

    logging.info("Key Data Skew Test Results: %s", data_skew_results)

    # Instantiate the IamClient class.
    iam_client = IamClient(iam_config=cc_credential)

    # Delete all the Kafka Cluster API keys created for each Kafka Cluster instance
    for kafka_credential in kafka_credentials:
        http_status_code, error_message = iam_client.delete_api_key(api_key=kafka_credential["sasl.username"])
        if http_status_code != HttpStatus.NO_CONTENT:
            logging.warning("FAILED TO DELETE KAFKA CLUSTER API KEY %s FOR KAFKA CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'], error_message)
        else:
            logging.info("Kafka Cluster API key %s for Kafka Cluster %s deleted successfully.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'])


# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()