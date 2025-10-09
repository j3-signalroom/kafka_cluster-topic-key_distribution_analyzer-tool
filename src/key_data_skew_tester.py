from collections import defaultdict
import json
import logging
import time
from typing import Dict
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import streamlit as st
import plotly.graph_objects as go

from constants import (DEFAULT_CHARACTER_REPEAT, 
                       DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                       DEFAULT_KAFKA_TOPIC_RECORD_COUNT,
                       DEFAULT_KAFKA_TOPIC_NAME,
                       DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                       DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS)
from utilities import setup_logging, create_topic_if_not_exists


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


class KeyDataSkewTester:
    """Class to test key/data skew in a Kafka cluster."""

    def __init__(self, kafka_cluster_id: str, bootstrap_server_uri: str, kafka_api_key: str, kafka_api_secret: str):
        """Connect to the Kafka Cluster with the AdminClient.

        Args:
            kafka_cluster_id (string): Your Confluent Cloud Kafka Cluster ID
            bootstrap_server_uri (string): Kafka Cluster URI
            kafka_api_key (string): Your Confluent Cloud Kafka API key
            kafka_api_secret (string): Your Confluent Cloud Kafka API secret
        """
        # Create skewed data (80% of messages go to same key pattern)
        self.skewed_partition_mapping = defaultdict(list)
        
        self.kafka_cluster_id = kafka_cluster_id
        self.bootstrap_server_uri = bootstrap_server_uri
        self.kafka_api_key = kafka_api_key
        self.kafka_api_secret = kafka_api_secret

        # Instantiate the AdminClient with the provided credentials
        config = {
            'bootstrap.servers': bootstrap_server_uri,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': kafka_api_key,
            'sasl.password': kafka_api_secret,
        }
        self.admin_client = AdminClient(config)

    def __delivery_report(self, error_message, record):
        """ Delivery report callback called (from Producer) on successful or failed delivery of message.
        
        Args:
            error_message: Error message if delivery failed, else None.
            record: The produced record.
        """
        try:
            self.skewed_partition_mapping[record.partition()].append(record.key().decode('utf-8'))
        except Exception as e:
            logging.error(f"Error Message, {error_message} in delivery callback: {e}")

    def run_test(self,
                 topic_name=DEFAULT_KAFKA_TOPIC_NAME, 
                 partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT, 
                 replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR, 
                 data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS, 
                 record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT) -> Dict:
        """Run the Key Data Skew test.

        Arg(s):
            topic_name (str): Name of the Kafka topic to create/use.
            partition_count (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            data_retention_in_days (int): Data retention period in days for the topic.
            record_count (int): Number of records to produce.

        Return(s):
            Dict: Results of the Key Data Skew test.
        """
        logging.info("="*DEFAULT_CHARACTER_REPEAT)
        logging.info("Testing with skewed key distribution...")

        # 1. Create topic
        create_topic_if_not_exists(self.admin_client,
                                   topic_name,
                                   partition_count, 
                                   replication_factor, 
                                   data_retention_in_days)

        # Test skewed distribution
        producer = Producer({
            'bootstrap.servers': self.bootstrap_server_uri,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': self.kafka_api_key,
            'sasl.password': self.kafka_api_secret,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 10,
            'batch.size': 16384
        })

        # Initialize StringSerializer
        string_serializer = StringSerializer('utf_8')

        for id in range(500):
            # 80% of messages use the same key
            if id < 400:
                key_str = "hot-key-1"
            else:
                key_str = f"cold-key-{id}"
            
            serialized_key = string_serializer(key_str)

            # value
            value_dict = {
                "id": id,
                "key": key_str,
                "timestamp": time.time(),
                "data": f"test_record_{id}"
            }
            serialized_value = json.dumps(value_dict).encode('utf-8')

            # Produce record
            producer.produce(
                topic=topic_name,
                key=serialized_key,
                value=serialized_value,
                on_delivery=self.__delivery_report
            )

            logging.info(f"Skewed Distribution Produced record with key: {key_str}")
        
        producer.flush()
        
        # Analyze skewed distribution
        return {p: len(keys) for p, keys in self.skewed_partition_mapping.items()}

    def visualize_data_skew(self, partition_record_counts: Dict[int, int], title: str) -> None:
        """Visualize the key/data skew distribution using Plotly and Streamlit.

        Args:
            partition_record_counts (Dict[int, int]): Dictionary mapping partition numbers to record counts.
            title (str): Title for the plot.

        Returns:
            None
        """
        # Prepare data for plotting
        partitions = list(partition_record_counts.keys())
        counts = list(partition_record_counts.values())

        # Calculate statistics
        avg_count = sum(counts) / len(counts)
        
        # Create Plotly figure
        fig = go.Figure()
        
        # Add bar chart
        fig.add_trace(go.Bar(
            x=partitions,
            y=counts,
            text=[f'{int(count)}' for count in counts],
            textposition='outside',
            marker_color='skyblue',
            marker_line_color='navy',
            marker_line_width=1.5,
            name='Records'
        ))
        
        # Add average line
        fig.add_hline(
            y=avg_count,
            line_dash="dash",
            line_color="red",
            annotation_text=f'Average: {avg_count:.1f}',
            annotation_position="top right"
        )
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title='Partition',
            yaxis_title='Number of Records',
            showlegend=True,
            height=600,
            yaxis=dict(gridcolor='lightgray', gridwidth=0.5, griddash='dot'),
            plot_bgcolor='white'
        )
        
        # Display in Streamlit
        st.plotly_chart(fig, use_container_width=True)
