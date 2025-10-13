import os
from typing import Dict, List, Tuple
from dotenv import load_dotenv
import logging
import streamlit as st
import ast

from utilities import setup_logging
from key_distribution_analyzer import KeyDistributionAnalyzer
from confluent_credentials import (fetch_confluent_cloud_credential_via_env_file,
                                   fetch_kafka_credentials_via_confluent_cloud_api_key)
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_KAFKA_TOPIC_NAME,
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
              topic_name: str, 
              key_pattern: List[str], 
              partition_count: int, 
              record_count: int,
              key_simulation_name: str,
              key_simulation_type: int) -> Tuple[bool, str]:
    """Run the Key Distribution and Data Skew tests.

    Arg(s):
        kafka_cluster (Dict): Kafka cluster information dictionary.
        topic_name (str): Kafka producer topic name.
        key_pattern (List[str]): List of key patterns to use.
        partition_count (int): Number of partitions for the producer topic.
        record_count (int): Number of records to produce.
        key_simulation_name (str): Name of the key simulation to use.
        key_simulation_type (int): Key simulation type index.

    Return(s):
        Tuple containing:
            - bool: True if tests ran successfully, False otherwise.
            - str: Error message if any.
    """
    # Initialize Key Distribution Tester
    distribution_test = KeyDistributionAnalyzer(kafka_cluster_id=kafka_cluster['kafka_cluster_id'],
                                              bootstrap_server_uri=kafka_cluster['bootstrap.servers'],
                                              kafka_api_key=kafka_cluster['sasl.username'],
                                              kafka_api_secret=kafka_cluster['sasl.password'])

    # Run Key Distribution Test
    distribution_results, error_message = distribution_test.run_test(st,
                                                                     topic_name=topic_name,
                                                                     partition_count=partition_count,
                                                                     record_count=record_count,
                                                                     key_pattern=key_pattern,
                                                                     replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                                     data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                                                     key_simulation_name=key_simulation_name,
                                                                     key_simulation_type=key_simulation_type)
    if not distribution_results:
        return False, error_message

    logging.info("Key Distribution Analysis Results: %s", distribution_results)

    return True, ""


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
    progress_bar = st.progress(0, text="Start the deletion process...")
    for index, kafka_credential in enumerate(kafka_credentials.values()):
        http_status_code, error_message = iam_client.delete_api_key(api_key=kafka_credential["sasl.username"])
        if http_status_code != HttpStatus.NO_CONTENT:
            logging.warning("FAILED TO DELETE KAFKA CLUSTER API KEY %s FOR KAFKA CLUSTER %s BECAUSE THE FOLLOWING ERROR OCCURRED: %s.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'], error_message)
        else:
            logging.info("Kafka Cluster API key %s for Kafka Cluster %s deleted successfully.", kafka_credential["sasl.username"], kafka_credential['kafka_cluster_id'])
        progress_bar.progress(min((index + 1) * 100 // len(kafka_credentials), 100), text=f"Deleted Kafka Cluster API key {kafka_credential['sasl.username']} for Kafka Cluster {kafka_credential['kafka_cluster_id']}.")
    progress_bar.progress(100, text="Deletion process completed.")


def main():
    """Main tool entry point."""

    st.set_page_config(layout="wide")
    if 'true_or_false' not in st.session_state:
        st.session_state['true_or_false'] = True

    # --- The title and subtitle of the tool
    st.title("Key Distribution Analyzer Tool Dashboard")
    st.write("This teaching tool shows you how the different key patterns, key simulation strategies, and partition strategies affect the key distribution.")

    # --- Fetch the environment and Kafka credentials
    cc_credential, environments, kafka_clusters, kafka_credentials = fetch_environment_with_kakfa_credentials()
    
    # --- Display the Environment and Kafka Cluster selection dropdowns
    if not cc_credential or not environments or not kafka_clusters or not kafka_credentials:
        logging.error("THE APPLICATION FAILED TO FETCH THE CONFLUENT ENVIRONMENT'S [YOU HAVE ACCESS TO] KAFKA CLUSTERS' KAFKA CREDENTIALS.")
        st.error("The Tool was unable to fetch the Confluent Environment's [you have access to] Kafka Clusters' Kafka Credentials. Please check that your Confluent Cloud API Key and Secret are correct and that you have access to at least one Kafka Cluster in a Confluent Environment.")
        st.stop()
    else:
        logging.info("Successfully fetched the Confluent Environment's [you have access to] Kafka Clusters' Kafka Credentials.")
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
                            options=[environment.get("display_name") for environment in environments.values()],
                            help="Select the Confluent Environment that contains the Kafka Cluster you want to use for the tests.",
                            disabled=not st.session_state['true_or_false']
                        )
                        selected_environment_id = {environment.get("id") for environment in environments.values() if environment.get("display_name") == selected_environment}
                    with col2:
                        selected_kafka_cluster = st.selectbox(
                            index=0,
                            label=f"Choose the {selected_environment}'s Kafka Cluster:",
                            options=[kafka_cluster.get("display_name") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("environment_id") in selected_environment_id],
                            help="Select the Kafka Cluster you want to use for the tests.",
                            disabled=not st.session_state['true_or_false']
                        )
                        selected_kafka_cluster_id = list({kafka_cluster.get("id") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("display_name") == selected_kafka_cluster})[0]

        with st.container(border=False):
            test_col1, test_col2 = st.columns(2)
            with test_col1:
                topic_name = st.text_input("Enter your Kafka Producer topic name:", 
                                           placeholder=DEFAULT_KAFKA_TOPIC_NAME,
                                           help="Enter the name of the Kafka topic to produce to.",
                                           disabled=not st.session_state['true_or_false'])
                button_disabled = not bool(topic_name.strip()) or not st.session_state['true_or_false']

                with st.container(border=True):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        key_pattern = st.text_input("Key Pattern:", 
                                                    placeholder=DEFAULT_KAFKA_TOPIC_KEY_PATTERN,
                                                    help="Enter a list of strings representing the key pattern. Example: ['tenant_id-', 'user_id-', 'object_id-']",
                                                    disabled=not st.session_state['true_or_false'])
                        button_disabled = button_disabled or not bool(key_pattern.strip())
                    with col2:
                        key_simulation_options = ["Normal", "Less Repetition", "More Repetition", "No Repetition", "Hot Key (data skew)"]
                        selected_key_simulation = st.selectbox(index=0,
                                                               label='Key Simulation:',
                                                               options=key_simulation_options,
                                                               help="Select the key simulation strategy to use.",
                                                               disabled=not st.session_state['true_or_false'])
                        selected_key_simulation_index = key_simulation_options.index(selected_key_simulation)
                    with col3:
                        partition_count = st.number_input("Partition Count:",
                                                          min_value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                          max_value=DEFAULT_KAFKA_TOPIC_MAXIMUM_PARTITION_COUNT,
                                                          value=DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT,
                                                          step=1,
                                                          help="Use arrows or type to change value",
                                                          disabled=not st.session_state['true_or_false'])                    
                    with col4:
                        selected_record_count = st.selectbox(index=2,
                                                             label='Record Count:',
                                                             options=["10", "100", "1,000", "10,000", "100,000"],
                                                             help="Select the number of records to produce.",
                                                             disabled=not st.session_state['true_or_false'])
        if st.button("Run Key Distribution Analysis Tests",
                     help="This will run the Key Distribution Analysis tests.",
                     type="primary",
                     disabled=button_disabled):
            st.session_state['true_or_false'] = False
            result, error_message = run_tests(kafka_credentials[selected_kafka_cluster_id],
                                              topic_name=topic_name,
                                              key_pattern=ast.literal_eval(key_pattern) if key_pattern else DEFAULT_KAFKA_TOPIC_KEY_PATTERN,
                                              partition_count=partition_count,
                                              record_count=int(selected_record_count.replace(",", "")),
                                              key_simulation_name=selected_key_simulation,
                                              key_simulation_type=selected_key_simulation_index)
            st.session_state['true_or_false'] = True
            if not result:
                st.error(error_message)
            else:
                st.balloons()

        st.divider()

        if st.button("Cleanup Resources before Closing the Tool", 
                     help="This will delete all the Kafka Cluster API keys created for each Kafka Cluster instance.", 
                     type="secondary", 
                     disabled=not st.session_state['true_or_false']):
            st.session_state['true_or_false'] = False
            delete_all_kafka_credentals_created(cc_credential, kafka_credentials)
            st.success("Cleanup completed. You can close the Tool now.")
            st.session_state['true_or_false'] = True
            st.snow()
            st.stop()
 



# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()