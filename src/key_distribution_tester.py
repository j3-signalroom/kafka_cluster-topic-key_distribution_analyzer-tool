import time
import json
import hashlib
from collections import defaultdict
from typing import Dict, Tuple
from confluent_kafka import Producer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.admin import AdminClient
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import logging

from utilities import setup_logging, create_topic_if_not_exists
from constants import (DEFAULT_KAFKA_TOPIC_PARTITION_COUNT,
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
            'group.id': f'key-distribution-tester-{int(time.time())}',
            'enable.auto.commit': False,
            'session.timeout.ms': 45000,
            'request.timeout.ms': 30000,
            'fetch.min.bytes': 1,
            'log_level': 3,            
            'enable.partition.eof': True,
            'fetch.message.max.bytes': 10485760, # 10MB max message size
            'queued.min.messages': 1000,     
            'enable.metrics.push': False,        # Disable metrics pushing for consumers to registered JMX MBeans.  However, is really being set to False to not expose unneccessary noise to the logging output
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': StringDeserializer('utf_8')
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
    
    def __produce_test_records(self, topic_name, record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT) -> None:
        """Produce test records with specific key patterns to the topic.

        Arg(s):
            topic_name (str): Kafka topic name.
            record_count (int): Number of records to produce.

        Return(s):
            None
        """
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

    def __analyze_distribution(self, partition_mapping: Dict[int, list]) -> Tuple[Dict[int, int], Dict[str, Dict[int, int]]]:
        """Analyze the distribution of keys across partitions and log the results.

        Arg(s):
            partition_mapping (Dict[int, list]): Mapping of partition numbers to lists of keys.

        Return(s):
            Tuple[Dict[int, int], Dict[str, Dict[int, int]]]: Partition record counts and key pattern distribution.
        """
        logging.info("=== Key Distribution Analysis ===")
        
        # Records per partition
        producer_partition_record_counts = {partition: len(keys) for partition, keys in partition_mapping.items()}
        total_records = sum(producer_partition_record_counts.values())

        logging.info("Total records: %d", total_records)
        logging.info("Number of partitions: %d", len(producer_partition_record_counts))

        for partition in sorted(producer_partition_record_counts.keys()):
            count = producer_partition_record_counts[partition]
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

        return producer_partition_record_counts, key_patterns
    
    def __test_hash_distribution(self, keys, partition_count) -> Dict[int, int]:
        """Test the theoretical hash distribution of keys across partitions.

        Arg(s):
            keys (list): List of keys to analyze.
            partition_count (int): Number of partitions.

        Return(s):
            Dict[int, int]: Theoretical hash distribution across partitions.
        """
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
    
    def __consume_and_analyze(self, topic_name: str):
        """Consume records from the topic and analyze the actual distribution.

        Args:
            topic_name (str): Kafka topic name.
            timeout_ms (int, optional): Consumer timeout in milliseconds. Defaults to DEFAULT_TOPIC_CONSUMER_TIMEOUT_MS.

        Returns:
            Dict[int, List[Dict]]: Consumed records grouped by partition.
        """
        consumer = DeserializingConsumer(self.kafka_consumer_config)
        consumer.subscribe([topic_name])
        
        partition_data = defaultdict(list)

        logging.info("Consuming records from topic '%s'...", topic_name)

        try:
            for record in consumer:
                partition_data[record.partition].append({
                    'key': record.key().decode('utf-8') if record.key() else None,
                    'value':  json.loads(record.value().decode('utf-8')) if record.value() else None,
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
    
    def run_test(self,
                 topic_name=DEFAULT_KAFKA_TOPIC_NAME, 
                 partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT, 
                 replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR, 
                 data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS, 
                 record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT) -> Dict:
        """Run the Key Distribution Test.
        Arg(s):
            topic_name (str): Kafka topic name.
            partition_count (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            data_retention_in_days (int): Data retention period in days.
            record_count (int): Number of records to produce for the test.

        Return(s):
            Dict: Test results including distribution metrics and visualizations.
        """
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
        producer_partition_record_counts, key_patterns = self.__analyze_distribution(self.partition_mapping)

        # 4. Test hash distribution
        all_keys = []
        for keys in self.partition_mapping.values():
            all_keys.extend(keys)
        
        hash_distribution = self.__test_hash_distribution(all_keys, partition_count)

        consumer_partition_record_counts = None
        logging.info("=== Consumer Verification ===")
        logging.info("Consuming messages to verify actual distribution...")
        
        partition_data = self.__consume_and_analyze(topic_name)
        
        # Calculate consumer-verified counts
        consumer_partition_record_counts = {
            partition: len(messages) 
            for partition, messages in partition_data.items()
        }
        
        # Compare producer vs consumer counts
        logging.info("=== Producer vs Consumer Comparison ===")
        total_producer = sum(producer_partition_record_counts.values())
        total_consumer = sum(consumer_partition_record_counts.values())

        logging.info("Producer reported: %d messages", total_producer)
        logging.info("Consumer verified: %d messages", total_consumer)
        
        if total_producer == total_consumer:
            logging.info("✅ All messages accounted for")
        else:
            logging.warning("⚠️ Mismatch: %d messages missing", total_producer - total_consumer)

        logging.info("Per-partition comparison:")
        for partition in sorted(producer_partition_record_counts.keys()):
            prod_count = producer_partition_record_counts.get(partition, 0)
            cons_count = consumer_partition_record_counts.get(partition, 0)
            match = "✅" if prod_count == cons_count else "⚠️"
            logging.info("Partition %d: Producer=%d, Consumer=%d %s", partition, prod_count, cons_count, match)
        
        # 5. Compare actual vs theoretical
        logging.info("=== Actual vs Theoretical Distribution ===")
        logging.info("Actual distribution (from producer):")
        for partition in sorted(producer_partition_record_counts.keys()):
            actual = producer_partition_record_counts[partition]
            theoretical = hash_distribution.get(partition, 0)
            logging.info("Partition %d: Actual=%d, Theoretical=%d", partition, actual, theoretical)
        
        # 6. Calculate distribution quality metrics
        producer_counts = list(producer_partition_record_counts.values())
        producer_std_dev = pd.Series(producer_counts).std()
        producer_mean_count = pd.Series(producer_counts).mean()
        producer_cv = (producer_std_dev / producer_mean_count) * 100  # Coefficient of variation

        logging.info("=== Producer Distribution Quality Metrics ===")
        logging.info("Mean records per partition: %.1f", producer_mean_count)
        logging.info("Standard deviation: %.1f", producer_std_dev)
        logging.info("Coefficient of variation: %.1f%%", producer_cv)
        logging.info("Distribution quality: %s", 'Good' if producer_cv < 20 else 'Poor')

        # 6. Calculate distribution quality metrics
        consumer_counts = list(consumer_partition_record_counts.values())
        consumer_std_dev = pd.Series(consumer_counts).std()
        consumer_mean_count = pd.Series(consumer_counts).mean()
        consumer_cv = (consumer_std_dev / consumer_mean_count) * 100  # Coefficient of variation

        logging.info("=== Consumer Distribution Quality Metrics ===")
        logging.info("Mean records per partition: %.1f", consumer_mean_count)
        logging.info("Standard deviation: %.1f", consumer_std_dev)
        logging.info("Coefficient of variation: %.1f%%", consumer_cv)
        logging.info("Distribution quality: %s", 'Good' if consumer_cv < 20 else 'Poor')

        return {
            'producer_partition_record_counts': producer_partition_record_counts,
            'consumer_partition_record_counts': consumer_partition_record_counts,
            'key_patterns': key_patterns,
            'hash_distribution': hash_distribution,
            'producer_quality_metrics': {
                'mean': producer_mean_count,
                'std_dev': producer_std_dev,
                'cv': producer_cv
            },
            'consumer_quality_metrics': {
                'mean': consumer_mean_count,
                'std_dev': consumer_std_dev,
                'cv': consumer_cv
            }
        }
    
    def visualize_distribution(self, partition_record_counts: Dict[int, int], title: str) -> None:
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
