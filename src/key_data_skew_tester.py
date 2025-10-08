from collections import defaultdict
import json
import logging
import time
from typing import List
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
import matplotlib.pyplot as plt

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
        try:
            self.skewed_partition_mapping[record.partition()].append(record.key().decode('utf-8'))
        except Exception as e:
            logging.error(f"Error Message, {error_message} in delivery callback: {e}")

    def run_test(self,
                 topic_name=DEFAULT_KAFKA_TOPIC_NAME, 
                 partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT, 
                 replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR, 
                 data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS, 
                 record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT):
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
        skewed_counts = {p: len(keys) for p, keys in self.skewed_partition_mapping.items()}
        self.__visualize_distribution(skewed_counts, "Skewed Distribution Example")

    def __visualize_distribution(self, partition_counts: List, title: str):
        """Create visualization of partition distribution"""

        partitions = list(partition_counts.keys())
        counts = list(partition_counts.values())
        
        plt.figure(figsize=(10, 6))
        bars = plt.bar(partitions, counts, color='skyblue', edgecolor='navy')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom')
        
        plt.xlabel('Partition')
        plt.ylabel('Number of Records')
        plt.title(title)
        plt.grid(axis='y', alpha=0.3)
        
        # Calculate and display statistics
        avg_count = sum(counts) / len(counts)
        plt.axhline(y=avg_count, color='red', linestyle='--', 
                   label=f'Average: {avg_count:.1f}')
        plt.legend()
        
        plt.tight_layout()
        plt.show()