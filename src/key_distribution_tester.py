import time
import json
import hashlib
from collections import defaultdict
from typing import Dict
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.admin import AdminClient
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import logging

from utilities import setup_logging, create_topic_if_not_exists
from constants import (DEFAULT_TOPIC_CONSUMER_TIMEOUT_MS,
                       DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
                       DEFAULT_KAFKA_TOPIC_RECORD_COUNT,
                       DEFAULT_KAFKA_TOPIC_NAME,
                       DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR,
                       DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


class KeyDistributionTester:
    """Class to test and analyze key distribution in Kafka topics."""

    def __init__(self, kafka_cluster_id: str, bootstrap_server_uri: str, kafka_api_key: str, kafka_api_secret: str):
        """Connect to the Kafka Cluster with the AdminClient.

        Args:
            kafka_cluster_id (string): Your Confluent Cloud Kafka Cluster ID
            bootstrap_server_uri (string): Kafka Cluster URI
            kafka_api_key (string): Your Confluent Cloud Kafka API key
            kafka_api_secret (string): Your Confluent Cloud Kafka API secret
        """
        self.kafka_cluster_id = kafka_cluster_id
        self.bootstrap_server_uri = bootstrap_server_uri

        # Instantiate the AdminClient with the provided credentials
        config = {
            'bootstrap.servers': bootstrap_server_uri,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': kafka_api_key,
            'sasl.password': kafka_api_secret,
        }
        self.admin_client = AdminClient(config)

        # Setup the Kafka Consumer config
        self.kafka_consumer_config = {
            **config,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'session.timeout.ms': 45000,
            'request.timeout.ms': 30000,
            'fetch.min.bytes': 1,
            'log_level': 3,            
            'enable.partition.eof': True,
            'fetch.message.max.bytes': 10485760, # 10MB max message size
            'queued.min.messages': 1000,     
            'enable.metrics.push': False         # Disable metrics pushing for consumers to registered JMX MBeans.  However, is really being set to False to not expose unneccessary noise to the logging output
        }

        # Setup the Kafka Producer config
        self.kafka_producer_config = {
            **config,
            'acks': 'all',
            'retries': 5,
            'linger.ms': 10,
            'batch.size': 16384
        }

        self.partition_mapping = defaultdict(list)

    def __delivery_callback(self, error_message: str, record) -> None:
        """Callback invoked when a message is delivered or fails.

        Args:
            error_message (str): Error information if delivery failed, else None.
            record: The message that was produced.

        Return(s):
            None
        """
        try:
            self.partition_mapping[record.partition()].append(record.key().decode('utf-8'))
        except Exception as e:
            logging.error(f"Error Message, {error_message} in delivery callback: {e}")
    
    def __produce_test_records(self, topic_name, record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT):
        """Produce test records with different key patterns"""

        # Initialize StringSerializer
        string_serializer = StringSerializer('utf_8')
        producer = Producer(self.kafka_producer_config)
        key_patterns = ["user-", "order-", "event-"]

        logging.info("Producing %d records...", record_count)

        for id in range(record_count):
            try:
                # key
                key_pattern = key_patterns[id % len(key_patterns)]
                key_str = f"{key_pattern}{id % 100}"
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
                    on_delivery=self.__delivery_callback
                )
                producer.poll(0)
            except BufferError:
                logging.warning("Local producer queue is full (%d messages awaiting delivery): try again", len(producer))
                producer.poll(1)
                # Retry producing the record after polling
                producer.produce(
                    topic=topic_name,
                    key=serialized_key,
                    value=serialized_value,
                    on_delivery=self.__delivery_callback
                )
            except Exception as e:
                logging.error("Error producing record %d: %s", id, e)
        producer.flush()
    
    def __analyze_distribution(self, partition_mapping):
        """Analyze key distribution across partitions"""
        logging.info("=== Key Distribution Analysis ===")
        
        # Records per partition
        partition_record_counts = {partition: len(keys) for partition, keys in partition_mapping.items()}
        total_records = sum(partition_record_counts.values())

        logging.info("Total records: %d", total_records)
        logging.info("Number of partitions: %d", len(partition_record_counts))

        for partition in sorted(partition_record_counts.keys()):
            count = partition_record_counts[partition]
            percentage = (count / total_records) * 100
            logging.info("Partition %d: %d records (%.1f%%)", partition, count, percentage)

        # Key pattern distribution
        logging.info("=== Key Pattern Distribution ===")
        key_patterns = defaultdict(lambda: defaultdict(int))
        
        for partition, keys in partition_mapping.items():
            for key in keys:
                pattern = key.split('-')[0] + '-'
                key_patterns[pattern][partition] += 1
        
        for pattern, partitions in key_patterns.items():
            logging.info("Pattern '%s':", pattern)
            for partition in sorted(partitions.keys()):
                count = partitions[partition]
                logging.info("  Partition %d: %d records", partition, count)

        return partition_record_counts, key_patterns
    
    def __test_hash_distribution(self, keys, partition_count):
        """Test how keys would be distributed using default hash function"""

        logging.info("=== Hash Function Distribution Test ===")
        
        hash_distribution = defaultdict(int)
        
        for key in keys:
            # Simulate Kafka's default partitioning
            key_bytes = key.encode('utf-8')
            hash_value = hashlib.md5(key_bytes).hexdigest()
            partition = int(hash_value, 16) % partition_count
            hash_distribution[partition] += 1

        logging.info("Theoretical hash distribution:")
        for partition in sorted(hash_distribution.keys()):
            count = hash_distribution[partition]
            percentage = (count / len(keys)) * 100
            logging.info("Partition %d: %d keys (%.1f%%)", partition, count, percentage)

        return hash_distribution
    
    def consume_and_analyze(self, topic_name, timeout_ms=DEFAULT_TOPIC_CONSUMER_TIMEOUT_MS):
        """Consume records and analyze actual distribution"""

        self.kafka_consumer_config["topic_name"] = topic_name
        self.kafka_consumer_config["consumer_timeout_ms"] = timeout_ms
        self.kafka_consumer_config["key_deserializer"] = lambda m: m.decode('utf-8') if m else None
        self.kafka_consumer_config["value_deserializer"] = lambda m: json.loads(m.decode('utf-8'))
        consumer = Consumer(self.kafka_consumer_config)
        
        partition_data = defaultdict(list)

        logging.info("Consuming records from topic '%s'...", topic_name)

        try:
            for record in consumer:
                partition_data[record.partition].append({
                    'key': record.key,
                    'value': record.value,
                    'offset': record.offset,
                    'timestamp': record.timestamp
                })
        except Exception as e:
            logging.error("Consumer timeout or error: %s", e)
        
        consumer.close()
        
        # Analyze consumed data
        logging.info("Consumed data from %d partitions:", len(partition_data))
        for partition in sorted(partition_data.keys()):
            records = partition_data[partition]
            logging.info("Partition %d: %d records", partition, len(records))

        return partition_data

    def __visualize_distribution(self, partition_record_counts: Dict[int, int], title: str="Key Distribution Across Partitions") -> None:
        """Create visualization of partition distribution

        Args:
            partition_record_counts (Dict[int, int]): Dictionary with partition numbers as keys and record counts as values.
            title (str): Title of the plot.

        Return(s):
            None
        """
        partitions = list(partition_record_counts.keys())
        counts = list(partition_record_counts.values())
        
        avg_count = sum(counts) / len(counts)
        
        # Create Plotly figure
        fig = go.Figure()
        
        # Add bar chart
        fig.add_trace(go.Bar(
            x=partitions,
            y=counts,
            text=counts,
            textposition='outside',
            marker_color='skyblue',
            marker_line_color='navy',
            marker_line_width=1.5
        ))
        
        # Add average line
        fig.add_hline(
            y=avg_count,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Average: {avg_count:.1f}",
            annotation_position="right"
        )
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Partition",
            yaxis_title="Number of Records",
            showlegend=False,
            height=500
        )
        
        # Display in Streamlit
        st.plotly_chart(fig, use_container_width=True)
    
    def run_test(self,
                 topic_name=DEFAULT_KAFKA_TOPIC_NAME, 
                 partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT, 
                 replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR, 
                 data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS, 
                 record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT):
        """Run a comprehensive key distribution test"""
        logging.info("=== Kafka Key Distribution Comprehensive Test ===")
        
        # 1. Create topic
        create_topic_if_not_exists(self.admin_client,
                                   topic_name, 
                                   partition_count, 
                                   replication_factor, 
                                   data_retention_in_days)
        
        # 2. Produce records
        self.__produce_test_records(topic_name, record_count)
        
        # 3. Analyze distribution
        partition_record_counts, key_patterns = self.__analyze_distribution(self.partition_mapping)

        # 4. Test hash distribution
        all_keys = []
        for keys in self.partition_mapping.values():
            all_keys.extend(keys)
        
        hash_distribution = self.__test_hash_distribution(all_keys, partition_count)
        
        # 5. Compare actual vs theoretical
        logging.info("=== Actual vs Theoretical Distribution ===")
        logging.info("Actual distribution (from producer):")
        for partition in sorted(partition_record_counts.keys()):
            actual = partition_record_counts[partition]
            theoretical = hash_distribution.get(partition, 0)
            logging.info("Partition %d: Actual=%d, Theoretical=%d", partition, actual, theoretical)
        
        # 6. Visualize results
        self.__visualize_distribution(partition_record_counts, f"Actual Distribution - {topic_name}")
        
        # 7. Calculate distribution quality metrics
        counts = list(partition_record_counts.values())
        std_dev = pd.Series(counts).std()
        mean_count = pd.Series(counts).mean()
        cv = (std_dev / mean_count) * 100  # Coefficient of variation

        logging.info("=== Distribution Quality Metrics ===")
        logging.info("Mean records per partition: %.1f", mean_count)
        logging.info("Standard deviation: %.1f", std_dev)
        logging.info("Coefficient of variation: %.1f%%", cv)
        logging.info("Distribution quality: %s", 'Good' if cv < 20 else 'Poor')

        return {
            'partition_record_counts': partition_record_counts,
            'key_patterns': key_patterns,
            'hash_distribution': hash_distribution,
            'quality_metrics': {
                'mean': mean_count,
                'std_dev': std_dev,
                'cv': cv
            }
        }
