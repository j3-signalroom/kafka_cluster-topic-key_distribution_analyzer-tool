import os
from typing import Dict
from dotenv import load_dotenv
import logging
import streamlit as st

from utilities import setup_logging
from key_distribution_tester import KeyDistributionTester
from key_data_skew_tester import KeyDataSkewTester
from confluent_credentials import (fetch_confluent_cloud_credential_via_env_file,
                                   fetch_kafka_credentials_via_confluent_cloud_api_key)
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus
from constants import (DEFAULT_KAFKA_TOPIC_NAME,
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


@st.cache_data(ttl=900, show_spinner="Fetching the Confluent Environment's [you have access to] Kafka Clusters' Kafka Credentials...")
def load_environment_with_kakfa_credentials() -> tuple[Dict, Dict, Dict, Dict]:
    """Load the environment and Kafka credentials.

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


def run_tests(kafka_cluster: Dict) -> None:
    """Run the Key Distribution and Data Skew tests.

    Arg(s):
        kafka_cluster (Dict): Kafka cluster information dictionary.

    Return(s):
        None
    """
    # Initialize Key Distribution Tester
    distribution_test = KeyDistributionTester(kafka_cluster_id=kafka_cluster['kafka_cluster_id'],
                                              bootstrap_server_uri=kafka_cluster['bootstrap.servers'],
                                              kafka_api_key=kafka_cluster['sasl.username'],
                                              kafka_api_secret=kafka_cluster['sasl.password'])

    # Run Key Distribution Test
    distribution_results = distribution_test.run_test(topic_name=DEFAULT_KAFKA_TOPIC_NAME,
                                                      partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                                                      replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                      data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                                      record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT)

    logging.info("Key Distribution Test Results: %s", distribution_results)

    # Initialize Key Data Skew Tester
    data_skew_test = KeyDataSkewTester(kafka_cluster_id=kafka_cluster['kafka_cluster_id'],
                                       bootstrap_server_uri=kafka_cluster['bootstrap.servers'],
                                       kafka_api_key=kafka_cluster['sasl.username'],
                                       kafka_api_secret=kafka_cluster['sasl.password'])

    # Run Key Data Skew Test
    data_skew_results = data_skew_test.run_test(topic_name=DEFAULT_KAFKA_TOPIC_NAME,
                                                partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                                                replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                                                data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS,
                                                record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT)

    logging.info("Key Data Skew Test Results: %s", data_skew_results)


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
    st.title("Key Distribution Analyzer Dashboard")
    st.write("This Analyzer Tool displays the result of the Key Distribution and Data Skew analysis.")

    # --- Fetch the environment and Kafka credentials
    cc_credential, environments, kafka_clusters, kafka_credentials = load_environment_with_kakfa_credentials()
    
    # --- Create and fill in the two dropdown boxes used to determine the working Kafka cluster
    selected_environment = st.selectbox(
        index=0, 
        label='Choose the Environment:',
        options=[environment.get("display_name") for environment in environments.values()]
    )
    selected_environment_id = {environment.get("id") for environment in environments.values() if environment.get("display_name") == selected_environment}
    selected_kafka_cluster = st.selectbox(
        index=0,
        label="Choose the Environment's Kafka Cluster:",
        options=[kafka_cluster.get("display_name") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("environment_id") in selected_environment_id]
    )
    selected_kafka_cluster_id = list({kafka_cluster.get("id") for kafka_cluster in kafka_clusters.values() if kafka_cluster.get("display_name") == selected_kafka_cluster})[0]


    # --- Container with two sections (columns) to display the bar chart and pie chart
    with st.container(border=True):    
        col1, col2 = st.columns(2)

        with col1:
            # --- Bar chart flight count by departure month for the selected airline and year
            st.header("Airline Flights")
            st.title(f"{selected_airline} Monthly Flights in {selected_departure_year}")
            st.bar_chart(data=df_airline_monthly_flights_table[(df_airline_monthly_flights_table['departure_year'] == selected_departure_year) & (df_airline_monthly_flights_table['airline'] == selected_airline)] ,
                         x="departure_month_abbr",
                         y="flight_count",
                         x_label="Departure Month",
                         y_label="Number of Flights")
            st.write(f"This bar chart displays the number of {selected_airline} monthly flights in {selected_departure_year}.  The x-axis represents the month and the y-axis represents the number of flights.")

        with col2:
            # --- Pie chart top airports by departures for the selected airline and year
            # --- Create a slider to select the number of airports to rank
            st.header("Airport Ranking")
            st.title(f"Top {selected_departure_year} {selected_airline} Airports")
            df_filter_table = df_ranked_airports_table[(df_ranked_airports_table['airline'] == selected_airline) & (df_ranked_airports_table['departure_year'] == selected_departure_year)]
            rank_value = st.slider(label="Ranking:",
                                   min_value=3,
                                   max_value=df_filter_table['row_num'].max(), 
                                   step=1,
                                   value=3)
            fig = px.pie(df_filter_table[(df_filter_table['row_num'] <= rank_value)], 
                         values='flight_count', 
                         names='departure_airport_code', 
                         title=f"Top {rank_value} based on departures",)
            st.plotly_chart(fig, theme=None)
            st.write(f"This pie chart displays the top {rank_value} airports with the most departures for {selected_airline}.  The chart shows the percentage of flights departing from each of the top {rank_value} airports.")




    
    
    if st.button("Click Me"):
        result = run_tests(kafka_credentials[selected_kafka_cluster_id])
        st.success(result)
        st.balloons()

    if st.button("Exit"):
        delete_all_kafka_credentals_created(cc_credential, kafka_credentials)
        st.success("Cleanup completed. Exiting the application.")
        st.stop()


# Run the main function if this script is executed directly    
if __name__ == "__main__":
    main()