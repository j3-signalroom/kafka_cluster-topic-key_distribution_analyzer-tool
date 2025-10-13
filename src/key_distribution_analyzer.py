from enum import IntEnum
import time
import json
from collections import defaultdict
from typing import Dict, List, Tuple
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.admin import AdminClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import streamlit

from utilities import setup_logging, create_topic_if_not_exists


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


# Key Simulation Types
class KeySimulationType(IntEnum):
    NORMAL = 0
    LESS_REPETITION = 1
    MORE_REPETITION = 2
    NO_REPETITION = 3
    HOT_KEY_DATA_SKEW = 4


class KeyDistributionAnalyzer:
    """Class to test and analyze key distribution in Kafka topics."""

    def __init__(self, 
                 kafka_cluster_id: str, 
                 bootstrap_server_uri: str, 
                 kafka_api_key: str, 
                 kafka_api_secret: str):
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
            'auto.offset.reset': 'earliest',
            'group.id': f'key-distribution-tester-{int(time.time())}',
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

    def __produce_test_records(self, topic_name, record_count: int, key_pattern: List[str], key_simulation_type: KeySimulationType) -> None:
        """Produce test records with specific key patterns to the topic.
        Note: The modulo operations ensure the pattern cycles predictably, making it perfect
              for testing Kafka's key-based partitioning behavior.

        Arg(s):
            topic_name (str): Kafka topic name.
            record_count (int): Number of records to produce.
            key_pattern (List[str]): List of key patterns to use.
            key_simulation_type (KeySimulationType): Type of key simulation to use.

        Return(s):
            None
        """
        # Initialize StringSerializer
        string_serializer = StringSerializer('utf_8')
        producer = Producer(self.kafka_producer_config)
        key_patterns = key_pattern

        logging.info("Producing %d records...", record_count)

        for id in range(record_count):
            try:
                match key_simulation_type:
                    case KeySimulationType.NORMAL:
                        key_pattern = key_patterns[id % len(key_patterns)]
                        key_str = f"{key_pattern}{id % 100}"
                    case KeySimulationType.LESS_REPETITION:
                        key_pattern = key_patterns[id % len(key_patterns)]
                        key_str = f"{key_pattern}{id % 1000}"
                    case KeySimulationType.MORE_REPETITION:
                        key_pattern = key_patterns[id % len(key_patterns)]
                        key_str = f"{key_pattern}{id % 10}"
                    case KeySimulationType.NO_REPETITION:
                        key_pattern = key_patterns[id % len(key_patterns)]
                        key_str = f"{key_pattern}{id}"
                    case KeySimulationType.HOT_KEY_DATA_SKEW:
                        # 80% of records use the same key
                        if id < int(record_count * 0.8):
                            key_str = "hot-key"
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

    def __test_partition_strategies(self, keys, partition_count: int):
        """Test all Kafka partition strategies"""
        logging.info("=== Partition Strategy Comparison ===")
        
        strategies = {
            'default_(murmur_hash_2)': self.__murmur2_hash_strategy,
            'round_robin': self.__round_robin_strategy,
            'sticky': self.__sticky_strategy,
            'range_based_custom': self.__range_based_customer_strategy,
            'custom': self.__custom_strategy
        }
        
        results = {}
        
        for strategy_name, strategy_func in strategies.items():
            logging.info("%s Strategy ---", strategy_name.upper().replace('_', ' '))
            distribution = strategy_func(keys, partition_count)
            results[strategy_name] = distribution
            
            # Print distribution
            for partition in sorted(distribution.keys()):
                count = distribution[partition]
                percentage = (count / len(keys)) * 100
                logging.info(f"Partition {partition}: {count} keys ({percentage:.1f}%)")
            
            # Calculate quality metrics
            counts = list(distribution.values())
            if len(counts) > 0:
                std_dev = pd.Series(counts).std()
                mean = pd.Series(counts).mean()
                cv = (std_dev / mean) * 100 if mean > 0 else 0
                logging.info("CV: %.1f%% - %s", cv, "✅ Good" if cv < 20 else "⚠️ Poor")
        
        return results
    
    def __test_hash_distribution(self, keys, partition_count) -> Dict[int, int] | None:
        """Test the theoretical hash distribution of keys across partitions.

        Arg(s):
            keys (list): List of keys to analyze.
            partition_count (int): Number of partitions.

        Return(s):
            Dict[int, int]: Theoretical hash distribution across partitions.
            None: If partition strategy type is not implemented. 
        """
        logging.info("=== Hash Function Distribution Test ===")
        hash_distribution = self.__murmur2_hash_strategy(keys, partition_count)

        logging.info("Theoretical hash distribution:")
        for partition in sorted(hash_distribution.keys()):
            count = hash_distribution[partition]
            percentage = (count / len(keys)) * 100
            logging.info("Partition %d: %d keys (%.1f%%)", partition, count, percentage)

        return hash_distribution

    def __murmur2_hash_strategy(self, keys: list, partition_count: int) -> Dict[int, int]:
        hash_distribution = defaultdict(int)
        
        for key in keys:
            # Simulate Kafka's default partitioning
            key_bytes = key.encode('utf-8')
            hash_value = self.__murmur2_hash(key_bytes)
            partition = (hash_value & 0x7fffffff) % partition_count
            hash_distribution[partition] += 1

        return hash_distribution

    def __murmur2_hash(self, key: bytes) -> int:
        """Compute the Murmur2 hash for a given key.

        Args:
            key (bytes): The key to hash.

        Returns:
            int: The Murmur2 hash value.
        """
        m = 0x5bd1e995
        seed = 0x9747b28c
        h = seed ^ len(key)

        for byte in key:
            k = byte
            k = (k * m) & 0xffffffff
            k ^= k >> 24
            k = (k * m) & 0xffffffff
            h = (h * m) & 0xffffffff
            h ^= k

        h ^= h >> 11
        h = (h * m) & 0xffffffff
        h ^= h >> 15

        return h
    
    def __round_robin_strategy(self, keys, partition_count: int):
        """Round-robin partitioning (used when key is null)"""
        distribution = defaultdict(int)
        
        for index, key in enumerate(keys):
            # Ignore the key, just use record order
            partition = index % partition_count
            distribution[partition] += 1
        
        return distribution
    
    def __sticky_strategy(self, keys, partition_count: int, batch_size=100, linger_ms=10):
        """
        Kafka 4 Uniform Sticky Partitioner (Enhanced)
        
        Kafka 4 improvements over 2.4:
        1. Better initial partition selection (randomized)
        2. Smarter partition switching (avoids current partition)
        3. Considers partition availability
        4. Optimized batch accumulation
        
        For null keys only! With keys, uses MurmurHash2.
        
        Args:
            keys: List of keys (ignored - sticky is for null keys)
            partition_count: Number of partitions
            batch_size: Messages per batch before switching (default: 100)
            linger_ms: Simulated linger time (affects batching)
        """
        import random
        
        distribution = defaultdict(int)
        
        # Kafka 4: Random initial partition selection
        current_partition = random.randint(0, partition_count - 1)
        messages_in_batch = 0
        
        for index in range(len(keys)):
            # Stick to current partition
            distribution[current_partition] += 1
            messages_in_batch += 1
            
            # Check if we should switch partitions
            should_switch = False
            
            # Condition 1: Batch size reached
            if messages_in_batch >= batch_size:
                should_switch = True
            
            # Condition 2: Simulate linger timeout (every N messages)
            # In real Kafka, this would be time-based
            elif linger_ms > 0 and index > 0 and index % (batch_size * 2) == 0:
                should_switch = True
            
            if should_switch:
                # Kafka 4: Smart partition selection
                current_partition = self.__select_next_partition_kafka4(
                    current_partition,
                    partition_count
                )
                messages_in_batch = 0
        
        return distribution
    
    def __select_next_partition_kafka4(self, current_partition: int, partition_count: int):
        """
        Kafka 4 partition selection algorithm
        
        Improvements over Kafka 2.4:
        - Avoids recently used partition
        - Random selection for better load distribution
        - Handles single partition edge case
        """
        import random
        
        if partition_count <= 1:
            return 0
        
        # Available partitions (exclude current)
        available = [p for p in range(partition_count) if p != current_partition]
        
        # Kafka 4: Weighted random selection
        # In production, this considers broker load, but we'll use simple random
        return random.choice(available)
    
    def __range_based_customer_strategy(self, keys, partition_count: int):
        """Range-based partitioning (partition by key ranges)"""
        distribution = defaultdict(int)
        
        # Sort unique keys and create ranges
        unique_keys = sorted(set(keys))
        range_size = len(unique_keys) // partition_count + 1
        
        # Create key-to-partition mapping
        key_to_partition = {}
        for index, key in enumerate(unique_keys):
            partition = min(index // range_size, partition_count - 1)
            key_to_partition[key] = partition
        
        # Distribute keys
        for key in keys:
            partition = key_to_partition[key]
            distribution[partition] += 1
        
        return distribution
    
    def __custom_strategy(self, keys, partition_count: int):
        """Simple modulo hash (not recommended for production)"""
        distribution = defaultdict(int)
        
        for key in keys:
            # Simple Python hash with modulo
            hash_value = hash(key)
            partition = abs(hash_value) % partition_count
            distribution[partition] += 1
        
        return distribution
    
    def __visualize_strategy_comparison(self, st: streamlit, strategy_results, partition_count: int) -> None:
        """Create side-by-side comparison of all partition strategies using Streamlit"""
        
        st.subheader('Kafka Partition Strategy Comparison')
        
        # Create subplots with Plotly (better for Streamlit than matplotlib)
        rows = 2
        cols = 3
        fig = make_subplots(
            rows=rows, 
            cols=cols,
            subplot_titles=[name.replace("_", " ").title() for name in strategy_results.keys()],
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )
        
        for idx, (strategy_name, distribution) in enumerate(strategy_results.items()):
            row = (idx // cols) + 1
            col = (idx % cols) + 1
            
            partitions = list(range(partition_count))
            counts = [distribution.get(p, 0) for p in partitions]
            
            # Add bar chart
            fig.add_trace(
                go.Bar(
                    x=partitions,
                    y=counts,
                    name=strategy_name,
                    text=[f'{int(c)}' if c > 0 else '' for c in counts],
                    textposition='outside',
                    marker=dict(color='skyblue', line=dict(color='navy', width=1)),
                    showlegend=False
                ),
                row=row, col=col
            )
            
            # Calculate metrics
            if len(counts) > 0 and sum(counts) > 0:
                avg_count = sum(counts) / len(counts)
                std_dev = pd.Series(counts).std()
                cv = (std_dev / avg_count) * 100 if avg_count > 0 else 0
                
                # Add average line
                fig.add_hline(
                    y=avg_count,
                    line_dash="dash",
                    line_color="red",
                    opacity=0.7,
                    row=row, col=col,
                    annotation_text=f'Avg: {avg_count:.1f}',
                    annotation_position="right"
                )
                
                # Update subplot title with CV and quality indicator
                quality = '✅' if cv < 20 else '⚠️'
                title_text = f'{strategy_name.replace("_", " ").title()}<br>CV: {cv:.1f}% {quality}'
                fig.layout.annotations[idx].update(text=title_text)
            
            # Update axes
            fig.update_xaxes(title_text='Partition', row=row, col=col)
            fig.update_yaxes(title_text='Messages', row=row, col=col, gridcolor='lightgray')
        
        # Update layout
        fig.update_layout(
            height=800,
            width=1400,
            showlegend=False
        )
        
        # Display in Streamlit
        st.plotly_chart(fig, use_container_width=True)
        
        summary_data = []
        for strategy_name, distribution in strategy_results.items():
            counts = [distribution.get(p, 0) for p in range(partition_count)]
            if len(counts) > 0 and sum(counts) > 0:
                avg_count = sum(counts) / len(counts)
                std_dev = pd.Series(counts).std()
                cv = (std_dev / avg_count) * 100 if avg_count > 0 else 0
                quality = '✅ Good' if cv < 20 else '⚠️ Needs Improvement'
                
                summary_data.append({
                    'Partition Strategy': strategy_name.replace("_", " ").title(),
                    'Total Records': sum(counts),
                    'Average per Partition': f'{avg_count:.1f}',
                    'Standard Deviation': f'{std_dev:.2f}',
                    'Coefficient of Variation (%)': f'{cv:.1f}',
                    'Quality': quality
                })
        
        if summary_data:
            st.subheader('Partition Strategy Metrics Summary')
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)

    def run_test(self,
                 st: streamlit,
                 topic_name: str, 
                 partition_count: int,
                 record_count: int, 
                 key_pattern: List[str],
                 replication_factor: int, 
                 data_retention_in_days: int,
                 key_simulation_type: KeySimulationType) -> Tuple[Dict | None, str]:
        """Run the Key Distribution Test.
        Arg(s):
            st (streamlit): Streamlit instance for visualization.
            topic_name (str): Kafka topic name.
            partition_count (int): Number of partitions for the topic.
            record_count (int): Number of records to produce for the test.
            key_pattern (List[str]): List of key patterns to use.
            replication_factor (int): Replication factor for the topic.
            data_retention_in_days (int): Data retention period in days.
            key_simulation_type (KeySimulationType): Type of key simulation to use.

        Return(s):
            Tuple containing:
                - Dict: Results of the Key Distribution test.
                - str: Error message if any.
                - None: If topic creation fails.
        """
        logging.info("=== Kafka Key Distribution Analysis ===")

        progress_bar = st.progress(0, text="Start Analyzing...")

        # 1. Create or recreate topic if it doesn't exist
        progress_bar.progress(0.125, text="Analyzing...  Create or recreate topic if it doesn't exist")
        if not create_topic_if_not_exists(self.admin_client, topic_name, partition_count, replication_factor, data_retention_in_days):
            logging.error("Failed to create or recreate topic '%s'. Aborting test.", topic_name)
            return None, f"Failed to create or recreate topic '{topic_name}'."

        # 2. Produce test records with specified key patterns
        progress_bar.progress(0.25, text="Analyzing...  Produce records with specified key patterns")
        self.__produce_test_records(topic_name, record_count, key_pattern, key_simulation_type)

        # 3. Analyze the distribution of keys across partitions
        progress_bar.progress(0.375, text="Analyzing...  Analyze the distribution of keys across partitions")
        producer_partition_record_counts, key_patterns = self.__analyze_distribution(self.partition_mapping)

        # 4. Test all partition strategies
        progress_bar.progress(0.5, text="Analyzing...  Test all partition strategies")
        all_keys = []
        for keys in self.partition_mapping.values():
            all_keys.extend(keys)
        strategy_results = self.__test_partition_strategies(all_keys, partition_count)
        self.__visualize_strategy_comparison(st, strategy_results, partition_count)

        # 5. Test theoretical hash distribution
        progress_bar.progress(0.625, text="Analyzing...  Theoretical hash distribution")
        hash_distribution = self.__test_hash_distribution(all_keys, partition_count)
        if hash_distribution is None:
            logging.error("Hash distribution test failed due to unimplemented partition strategy. Aborting test.")
            return None, "Hash distribution test failed due to unimplemented partition strategy."
        
        # 6. Compare actual vs theoretical distribution
        progress_bar.progress(0.75, text="Analyzing...  Compare actual vs theoretical distribution")
        logging.info("=== Actual vs Theoretical Distribution ===")
        logging.info("Actual distribution (from producer):")
        for partition in sorted(producer_partition_record_counts.keys()):
            actual = producer_partition_record_counts[partition]
            theoretical = hash_distribution.get(partition, 0)
            logging.info("Partition %d: Actual=%d, Theoretical=%d", partition, actual, theoretical)

        # 7. Calculate distribution quality metrics
        progress_bar.progress(0.875, text="Analyzing...  Calculate distribution quality metrics")
        producer_counts = list(producer_partition_record_counts.values())
        producer_std_dev = pd.Series(producer_counts).std()
        producer_mean_count = pd.Series(producer_counts).mean()
        producer_cv = (producer_std_dev / producer_mean_count) * 100  # Coefficient of variation

        logging.info("=== Producer Distribution Quality Metrics ===")
        logging.info("Mean records per partition: %.1f", producer_mean_count)
        logging.info("Standard deviation: %.1f", producer_std_dev)
        logging.info("Coefficient of variation: %.1f%%", producer_cv)
        logging.info("Distribution quality: %s", 'Good' if producer_cv < 20 else 'Poor')

        # 8. Finalize and return results
        progress_bar.progress(1.0, text="Analysis complete")
        return {
            'producer_partition_record_counts': producer_partition_record_counts,
            'key_patterns': key_patterns,
            'hash_distribution': hash_distribution,
            'producer_quality_metrics': {
                'mean': producer_mean_count,
                'standard_deviation': producer_std_dev,
                'coefficient_of_variation': producer_cv
            }
        }, ""
