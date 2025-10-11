import os
from typing import Dict, List
from dotenv import load_dotenv
import logging
import streamlit as st
import sys
import ast

from utilities import setup_logging
from key_distribution_tester import KeyDistributionTester
from key_data_skew_tester import KeyDataSkewTester
from confluent_credentials import (fetch_confluent_cloud_credential_via_env_file,
                                   fetch_kafka_credentials_via_confluent_cloud_api_key)
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_DISTRIBUTION_KAFKA_TOPIC_NAME,
                       DEFAULT_DATA_SKEW_KAFKA_TOPIC_NAME,
                       DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                       DEFAULT_KAFKA_TOPIC_MAXIMUM_PARTITION_COUNT,
                       DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                       DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                       DEFAULT_USE_AWS_SECRETS_MANAGER,
                       DEFAULT_KAFKA_TOPIC_KEY_PATTERN)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


@st.cache_data(ttl=900, show_spinner="Fetching the Confluent Environment's [you have access to] Kafka Clusters' Kafka Credentials...")
def fetch_environment_with_kakfa_credentials() -> tuple[Dict, Dict, Dict, Dict]:
    """Fetch the environment and Kafka credentials.

    Return(s):
        Tuple containing:
            - cc_credential (Dict): Confluent Cloud credential dictionary.
            - environments (Dict): Environments dictionary.
            - kafka_clusters (Dict): Kafka clusters dictionary.
            - kafka_credentials (Dict): Kafka credentials dictionary.
    """
    # Load environment variables from .env file
    load_dotenv()
 
    # Read configuration settings from environment variables
    try:
        environment_filter = os.getenv("ENVIRONMENT_FILTER")
        kafka_cluster_filter = os.getenv("KAFKA_CLUSTER_FILTER")
        principal_id = os.getenv("PRINCIPAL_ID")
        use_aws_secrets_manager = os.getenv("USE_AWS_SECRETS_MANAGER", DEFAULT_USE_AWS_SECRETS_MANAGER) == "True"
    except Exception as e:
        logging.error("THE APPLICATION FAILED TO READ CONFIGURATION SETTINGS BECAUSE OF THE FOLLOWING ERROR: %s", e)
        return None, None, None
    
    # Fetch Confluent Cloud credentials from environment variable or AWS Secrets Manager
    cc_credential = fetch_confluent_cloud_credential_via_env_file(use_aws_secrets_manager)
    if not cc_credential:
        return None, None, None, None

    # Fetch Kafka credentials
    environments, kafka_clusters, kafka_credentials = fetch_kafka_credentials_via_confluent_cloud_api_key(principal_id, cc_credential, environment_filter, kafka_cluster_filter)
    if not environments or not kafka_clusters or not kafka_credentials:
        return None, None, None, None
    else:
        return cc_credential, environments, kafka_clusters, kafka_credentials


def run_tests(kafka_cluster: Dict, 
              distribution_topic_name: str, 
              key_pattern: List[str], 
              distribution_partition_count: int, 
              distribution_record_count: int,
              data_skew_topic_name: str,
              data_skew_partition_count: int) -> bool:
    """Run the Key Distribution and Data Skew tests.

    Arg(s):
        kafka_cluster (Dict): Kafka cluster information dictionary.
        distribution_topic_name (str): Kafka producer topic name.
        key_pattern (List[str]): List of key patterns to use.
        distribution_partition_count (int): Number of partitions for the producer topic.
        distribution_record_count (int): Number of records to produce.
        data_skew_topic_name (str): Kafka consumer topic name for data skew test.
        data_skew_partition_count (int): Number of partitions for the data skew topic.

    Return(s):
        bool: True if tests ran successfully, False otherwise.
    """
    # Initialize Key Distribution Tester
    distribution_test = KeyDistributionTester(kafka_cluster_id=kafka_cluster['kafka_cluster_id'],
                                              bootstrap_server_uri=kafka_cluster['bootstrap.servers'],
                                              kafka_api_key=kafka_cluster['sasl.username'],
                                              kafka_api_secret=kafka_cluster['sasl.password'])

    # Run Key Distribution Test
    distribution_results = distribution_test.run_test(distribution_topic_name=distribution_topic_name,
                                                      distribution_partition_count=distribution_partition_count,
                                                      distribution_record_count=distribution_record_count,
                                                      key_pattern=key_pattern,
                                                      replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                      data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS)
    if not distribution_results:
        return False

    logging.info("Key Distribution Test Results: %s", distribution_results)

    # Initialize Key Data Skew Tester
    data_skew_test = KeyDataSkewTester(kafka_cluster_id=kafka_cluster['kafka_cluster_id'],
                                       bootstrap_server_uri=kafka_cluster['bootstrap.servers'],
                                       kafka_api_key=kafka_cluster['sasl.username'],
                                       kafka_api_secret=kafka_cluster['sasl.password'])

    # Run Key Data Skew Test
    data_skew_results = data_skew_test.run_test(data_skew_topic_name=data_skew_topic_name,
                                                data_skew_partition_count=data_skew_partition_count,
                                                replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS)
    if not data_skew_results:
        return False

    logging.info("Key Data Skew Test Results: %s", data_skew_results)

    # --- Container with two sections (columns) to display the bar chart and pie chart
    with st.container(border=True):
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Producer Key Distribution Test Results")
            distribution_test.visualize_distribution(distribution_results["producer_partition_record_counts"], f"Actual Distribution - {distribution_topic_name}")
            st.json(distribution_results["producer_quality_metrics"])

        with col2:
            st.subheader("Consumer Key Distribution Test Results")
            distribution_test.visualize_distribution(distribution_results["consumer_partition_record_counts"], f"Actual Distribution - {data_skew_topic_name}")
            st.json(distribution_results["consumer_quality_metrics"])

    # --- Container to display the data skew test results
    with st.container(border=True):
        st.subheader("Key Data Skew Test Results")
        data_skew_test.visualize_data_skew(data_skew_results, "Skewed Distribution Example")

    return True


def delete_all_kafka_credentals_created(cc_credential: Dict, kafka_credentials: Dict) -> None:
    """Delete all the Kafka Cluster API keys created for each Kafka Cluster instance.
    
    Arg(s):
        cc_credential (Dict): Confluent Cloud credential dictionary.
        kafka_credentials (Dict): Kafka credentials dictionary.
    Return(s):
        None
    """
    # Instantiate the IamClient class.
    iam_client = IamClient(iam_config=cc_credential)

    # Delete all the Kafka Cluster API keys created for each Kafka Cluster instance
    for kafka_credential in kafka_credentials.values():
        http_status_code, error_message = iam_client.delete_api_key(api_key=kafka_credential["sasl.username"])
        if http_status_code != HttpStatus.NO_CONTENT:
            logging.warning("FAILED TO DELETE KAFKA CLUSTER API KEY %s FOR KAFKA CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'], error_message)
        else:
            logging.info("Kafka Cluster API key %s for Kafka Cluster %s deleted successfully.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'])


def main():
    """Main tool entry point."""

    st.set_page_config(layout="wide")

    # --- The title and subtitle of the app
    st.title("Key Distribution and Hot Partition Analyzer Dashboard")
    st.write("This Analyzer Tool displays the result of the Key Distribution and Data Skew analysis.")

    # --- Fetch the environment and Kafka credentials
    cc_credential, environments, kafka_clusters, kafka_credentials = fetch_environment_with_kakfa_credentials()
    
    with st.container(border=False):
        col1, col2 = st.columns(2)

        with col1:
            with st.container(border=False):
                col1, col2 = st.columns(2)
                with col1:
                    # --- Create and fill in the two dropdown boxes used to determine the working Kafka cluster
                    selected_environment = st.selectbox(
                        index=0, 
                        label='Choose the Environment:',
                        options=[environment.get("display_name") for environment in environments.values()]
                    )
                    selected_environment_id = {environment.get("id") for environment in environments.values() if environment.get("display_name") == selected_environment}
                with col2:
                    selected_kafka_cluster = st.selectbox(
                        index=0,
                        label=f"Choose the {selected_environment}'s Kafka Cluster:",
                        options=[kafka_cluster.get("display_name") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("environment_id") in selected_environment_id]
                    )
                    selected_kafka_cluster_id = list({kafka_cluster.get("id") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("display_name") == selected_kafka_cluster})[0]

    with st.container(border=False):
        test_col1, test_col2 = st.columns(2)
        with test_col1:
            distribution_topic_name = st.text_input("Enter your topic name for both the producer and consumer:", placeholder=DEFAULT_DISTRIBUTION_KAFKA_TOPIC_NAME)
            with st.container(border=True):
                row2_dist_col1, row2_dist_col2, row_2_dist_col3 = st.columns(3)
                with row2_dist_col1:
                    key_pattern = st.text_input("Enter your key pattern to use by the producer:", placeholder=DEFAULT_KAFKA_TOPIC_KEY_PATTERN)
                with row2_dist_col2:
                    distribution_partition_count = st.number_input("Distribution Topic Partition Counter:",
                                                            min_value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                            max_value=DEFAULT_KAFKA_TOPIC_MAXIMUM_PARTITION_COUNT,
                                                            value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                            step=1,
                                                            help="Use arrows or type to change value")                    
                with row_2_dist_col3:
                    selected_distribution_record_count = st.selectbox(index=2,
                                                                    label='Choose number of records to produce:',
                                                                    options=["10", "100", "1,000", "10,000", "100,000"])
        with test_col2:
            data_skew_topic_name = st.text_input("Enter your topic name for the data skew consumer:", placeholder=DEFAULT_DATA_SKEW_KAFKA_TOPIC_NAME)
            with st.container(border=True):
                row2_ds_col1, row2_ds_col2, row_2_ds_col3 = st.columns(3)
                with row2_ds_col1:
                    data_skew_partition_count = st.number_input("Partition Counter:",
                                                                    min_value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                                    max_value=DEFAULT_KAFKA_TOPIC_MAXIMUM_PARTITION_COUNT,
                                                                    value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                                    step=1,
                                                                    help="Use arrows or type to change value")
    if st.button("Run Key Distribution and Hot Partition Analyzer Tests"):
        result = run_tests(kafka_credentials[selected_kafka_cluster_id],
                           distribution_topic_name=distribution_topic_name,
                           key_pattern=ast.literal_eval(key_pattern) if key_pattern else DEFAULT_KAFKA_TOPIC_KEY_PATTERN,
                           distribution_partition_count=distribution_partition_count,
                           distribution_record_count=int(selected_distribution_record_count.replace(",", "")),
                           data_skew_topic_name=data_skew_topic_name,
                           data_skew_partition_count=data_skew_partition_count)
        if not result:
            st.error("The tests failed. Please check the log file for more details.")
        else:
            st.balloons()

    if st.button("Cleanup"):
        delete_all_kafka_credentals_created(cc_credential, kafka_credentials)
        st.success("Cleanup completed. You can close the Tool now.")
        st.stop()
        sys.exit(0)


# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()