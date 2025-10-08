import time
import json
import hashlib
from collections import defaultdict
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
import matplotlib.pyplot as plt
import pandas as pd
import logging

from utilities import setup_logging
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

    def delivery_callback(self, error_message: str, record) -> None:
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
    
    def produce_test_records(self, topic_name, record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT):
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
                    on_delivery=self.delivery_callback
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
                    on_delivery=self.delivery_callback
                )
            except Exception as e:
                logging.error("Error producing record %d: %s", id, e)
        producer.flush()
    
    def analyze_distribution(self, partition_mapping):
        """Analyze key distribution across partitions"""
        logging.info("=== Key Distribution Analysis ===")
        
        # Records per partition
        partition_counts = {partition: len(keys) for partition, keys in partition_mapping.items()}
        total_records = sum(partition_counts.values())

        logging.info("Total records: %d", total_records)
        logging.info("Number of partitions: %d", len(partition_counts))

        for partition in sorted(partition_counts.keys()):
            count = partition_counts[partition]
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

        return partition_counts, key_patterns
    
    def test_hash_distribution(self, keys, partition_count):
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
    
    def visualize_distribution(self, partition_counts, title="Key Distribution Across Partitions"):
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
    
    def run_comprehensive_test(self,
                               topic_name=DEFAULT_KAFKA_TOPIC_NAME, 
                               partition_count=DEFAULT_KAFKA_TOPIC_PARTITION_COUNT, 
                               replication_factor=DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR, 
                               data_retention_in_days=DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS, 
                               record_count=DEFAULT_KAFKA_TOPIC_RECORD_COUNT):
        """Run a comprehensive key distribution test"""
        logging.info("=== Kafka Key Distribution Comprehensive Test ===")
        
        # 1. Create topic
        self.__create_topic_if_not_exists(topic_name, partition_count, replication_factor, data_retention_in_days)
        
        # 2. Produce records
        partition_mapping = self.produce_test_records(topic_name, record_count)
        
        # 3. Analyze distribution
        partition_counts, key_patterns = self.analyze_distribution(partition_mapping)
        
        # 4. Test hash distribution
        all_keys = []
        for keys in partition_mapping.values():
            all_keys.extend(keys)
        
        hash_distribution = self.test_hash_distribution(all_keys, partition_count)
        
        # 5. Compare actual vs theoretical
        logging.info("=== Actual vs Theoretical Distribution ===")
        logging.info("Actual distribution (from producer):")
        for partition in sorted(partition_counts.keys()):
            actual = partition_counts[partition]
            theoretical = hash_distribution.get(partition, 0)
            logging.info("Partition %d: Actual=%d, Theoretical=%d", partition, actual, theoretical)
        
        # 6. Visualize results
        self.visualize_distribution(partition_counts, 
                                  f"Actual Distribution - {topic_name}")
        
        # 7. Calculate distribution quality metrics
        counts = list(partition_counts.values())
        std_dev = pd.Series(counts).std()
        mean_count = pd.Series(counts).mean()
        cv = (std_dev / mean_count) * 100  # Coefficient of variation

        logging.info("=== Distribution Quality Metrics ===")
        logging.info("Mean records per partition: %.1f", mean_count)
        logging.info("Standard deviation: %.1f", std_dev)
        logging.info("Coefficient of variation: %.1f%%", cv)
        logging.info("Distribution quality: %s", 'Good' if cv < 20 else 'Poor')

        return {
            'partition_counts': partition_counts,
            'key_patterns': key_patterns,
            'hash_distribution': hash_distribution,
            'quality_metrics': {
                'mean': mean_count,
                'std_dev': std_dev,
                'cv': cv
            }
        }
    def __create_topic_if_not_exists(self, topic_name: str, partition_count: int, replication_factor: int, data_retention_in_days: int) -> None:
        """Create the results topic if it doesn't exist.

        Args:
            topic_name (str): Name of the Kafka topic.
            partition_count (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            data_retention_in_days (int): Data retention period in days.
        
        Return(s):
            None
        """
        # Check if topic exists
        topic_list = self.admin_client.list_topics(timeout=10)
        
        # If topic exists, verify retention policy
        retention_policy = '-1' if data_retention_in_days == 0 else str(data_retention_in_days * 24 * 60 * 60 * 1000)  # Convert days to milliseconds
        if topic_name in topic_list.topics:
            logging.info(f"Kafka topic '{topic_name}' already exists but will verify retention policy")

            # Update existing topic retention policy
            resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
            resource.set_config('retention.ms', retention_policy)
            self.admin_client.alter_configs([resource])
        else:        
            # Otherwise, create new topic
            logging.info(f"Creating Kafka topic '{topic_name}' with {partition_count} partitions")

            new_topic = NewTopic(topic=topic_name,
                                 num_partitions=partition_count,
                                 replication_factor=replication_factor,
                                 config={
                                     'cleanup.policy': 'delete',
                                     'retention.ms': retention_policy,
                                     'compression.type': 'lz4'
                                 })
            
            futures = self.admin_client.create_topics([new_topic])
            
            # Wait for topic creation
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic is created
                    logging.info(f"Topic '{topic}' created successfully")
                except Exception as e:
                    logging.error(f"Failed to create topic '{topic}': {e}")
                    raise
