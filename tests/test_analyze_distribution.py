import logging
import pytest
from unittest.mock import patch
from collections import defaultdict


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings (J3)"]
__maintainer__ = "Jeffrey Jonathan Jennings (J3)"
__email__      = "j3@thej3.com"
__status__     = "dev"
 

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def mock_kafka_config():
    """Fixture for Kafka configuration."""
    return {"bootstrap.servers": "localhost:9092"}

@pytest.fixture
def test_instance(mock_kafka_config):
    """Fixture for test class instance."""
    from src.key_distribution_analyzer import KeyDistributionAnalyzer
    instance = KeyDistributionAnalyzer("lkc-abc1234", "localhost:9092", "api_key", "api_secret")
    instance.kafka_producer_config = mock_kafka_config
    return instance

@pytest.fixture
def mock_logging():
    """Fixture for mocked logging."""
    with patch('src.key_distribution_analyzer.logging') as mock_log:
        yield mock_log


class TestAnalyzeDistribution:
    """Test suite for KeyDistributionAnalyzer.__analyze_distribution method."""

    def test_basic_distribution_analysis(self, test_instance, mock_logging):
        """Test basic distribution analysis with simple partition mapping."""
        partition_mapping = {
            0: ['user-1', 'user-2', 'user-3'],
            1: ['order-1', 'order-2'],
            2: ['product-1', 'product-2', 'product-3', 'product-4']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts
        assert record_counts == {0: 3, 1: 2, 2: 4}
        assert sum(record_counts.values()) == 9
        
        # Verify key patterns
        assert 'user-' in key_patterns
        assert 'order-' in key_patterns
        assert 'product-' in key_patterns
        
        assert key_patterns['user-'][0] == 3
        assert key_patterns['order-'][1] == 2
        assert key_patterns['product-'][2] == 4
        
        # Verify logging calls
        mock_logging.info.assert_any_call("=== Key Distribution Analysis ===")
        mock_logging.info.assert_any_call("Total records: %d", 9)
        mock_logging.info.assert_any_call("Number of partitions: %d", 3)

    def test_distribution_with_single_partition(self, test_instance, mock_logging):
        """Test distribution analysis with only one partition."""
        partition_mapping = {
            0: ['key-1', 'key-2', 'key-3', 'key-4', 'key-5']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts
        assert record_counts == {0: 5}
        assert len(record_counts) == 1
        
        # Verify key patterns
        assert len(key_patterns) == 1
        assert key_patterns['key-'][0] == 5
        
        # Verify 100% in single partition
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 0, 5, 100.0
        )

    def test_distribution_with_empty_partitions(self, test_instance, mock_logging):
        """Test distribution analysis with empty partition mapping."""
        partition_mapping = {}
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify empty results
        assert record_counts == {}
        assert len(key_patterns) == 0
        
        # Verify logging
        mock_logging.info.assert_any_call("Total records: %d", 0)
        mock_logging.info.assert_any_call("Number of partitions: %d", 0)

    def test_distribution_with_mixed_key_patterns(self, test_instance, mock_logging):
        """Test distribution with multiple key patterns across partitions."""
        partition_mapping = {
            0: ['user-1', 'order-1', 'user-2'],
            1: ['order-2', 'product-1', 'order-3'],
            2: ['user-3', 'user-4', 'product-2']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts
        assert record_counts == {0: 3, 1: 3, 2: 3}
        
        # Verify key patterns distribution
        assert key_patterns['user-'][0] == 2
        assert key_patterns['user-'][2] == 2
        assert 1 not in key_patterns['user-']
        
        assert key_patterns['order-'][0] == 1
        assert key_patterns['order-'][1] == 2
        
        assert key_patterns['product-'][1] == 1
        assert key_patterns['product-'][2] == 1

    def test_distribution_percentages(self, test_instance, mock_logging):
        """Test that percentages are calculated correctly."""
        partition_mapping = {
            0: ['key-' + str(i) for i in range(50)],   # 50%
            1: ['key-' + str(i) for i in range(30)],   # 30%
            2: ['key-' + str(i) for i in range(20)]    # 20%
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts
        assert record_counts[0] == 50
        assert record_counts[1] == 30
        assert record_counts[2] == 20
        
        # Verify percentage logging
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 0, 50, 50.0
        )
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 1, 30, 30.0
        )
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 2, 20, 20.0
        )

    def test_partition_sorting_in_logs(self, test_instance, mock_logging):
        """Test that partitions are logged in sorted order."""
        partition_mapping = {
            5: ['key-1'],
            2: ['key-2'],
            8: ['key-3'],
            1: ['key-4']
        }
        
        test_instance._KeyDistributionAnalyzer__analyze_distribution(partition_mapping)
        
        # Get all partition logging calls
        partition_calls = [
            call for call in mock_logging.info.call_args_list
            if len(call[0]) > 0 and "Partition" in str(call[0][0]) and "records (" in str(call[0][0])
        ]
        
        # Extract partition numbers from calls
        partition_numbers = [call[0][1] for call in partition_calls]
        
        # Verify they are in sorted order
        assert partition_numbers == [1, 2, 5, 8]

    def test_key_pattern_extraction(self, test_instance, mock_logging):
        """Test key pattern extraction from different key formats."""
        partition_mapping = {
            0: [
                'user-123',
                'order-abc-456',  # Multiple hyphens
                'product-xyz',
                'payment-001-final'
            ]
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify patterns are extracted correctly (first segment + '-')
        assert 'user-' in key_patterns
        assert 'order-' in key_patterns
        assert 'product-' in key_patterns
        assert 'payment-' in key_patterns
        
        # Each pattern should have 1 record in partition 0
        for pattern in ['user-', 'order-', 'product-', 'payment-']:
            assert key_patterns[pattern][0] == 1

    def test_hot_key_scenario(self, test_instance, mock_logging):
        """Test distribution with hot key (data skew) scenario."""
        partition_mapping = {
            0: ['hot-key'] * 80 + ['cold-key-1', 'cold-key-2'],  # 82 keys
            1: ['cold-key-' + str(i) for i in range(3, 10)],    # 7 keys
            2: ['cold-key-' + str(i) for i in range(10, 21)]    # 11 keys
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts show skew
        assert record_counts[0] == 82
        assert record_counts[1] == 7
        assert record_counts[2] == 11
        
        # Verify hot key pattern dominates partition 0
        assert key_patterns['hot-'][0] == 80
        assert key_patterns['cold-'][0] == 2
        assert key_patterns['cold-'][1] == 7
        assert key_patterns['cold-'][2] == 11

    def test_logging_structure(self, test_instance, mock_logging):
        """Test the complete logging structure and order."""
        partition_mapping = {
            0: ['user-1', 'user-2'],
            1: ['order-1']
        }
        
        test_instance._KeyDistributionAnalyzer__analyze_distribution(partition_mapping)
        
        # Get all logging calls
        log_calls = [call[0][0] for call in mock_logging.info.call_args_list]
        
        # Verify main sections are logged
        assert "=== Key Distribution Analysis ===" in log_calls
        assert "=== Key Pattern Distribution ===" in log_calls
        
        # Verify the order of sections
        dist_index = log_calls.index("=== Key Distribution Analysis ===")
        pattern_index = log_calls.index("=== Key Pattern Distribution ===")
        assert dist_index < pattern_index

    def test_key_pattern_sorting_in_logs(self, test_instance, mock_logging):
        """Test that key patterns log partitions in sorted order."""
        partition_mapping = {
            3: ['user-1'],
            0: ['user-2'],
            5: ['user-3'],
            1: ['user-4']
        }
        
        test_instance._KeyDistributionAnalyzer__analyze_distribution(partition_mapping)
        
        # Find the pattern logging calls
        pattern_calls = [
            call for call in mock_logging.info.call_args_list
            if len(call[0]) > 1 and "Partition" in str(call[0][0]) and call[0][0].startswith("  ")
        ]
        
        # Extract partition numbers
        partition_numbers = [call[0][1] for call in pattern_calls]
        
        # Verify sorted order
        assert partition_numbers == sorted(partition_numbers)

    @pytest.mark.parametrize("total_records,partition_count,expected_avg", [
        (100, 4, 25),
        (1000, 10, 100),
        (50, 5, 10),
        (7, 3, 2.33)  # Approximate
    ])
    def test_various_distribution_sizes(
        self, test_instance, mock_logging, total_records, partition_count, expected_avg
    ):
        """Test distribution analysis with various sizes."""
        # Create evenly distributed partitions
        records_per_partition = total_records // partition_count
        remainder = total_records % partition_count
        
        partition_mapping = {}
        record_idx = 0
        for p in range(partition_count):
            extra = 1 if p < remainder else 0
            count = records_per_partition + extra
            partition_mapping[p] = [f'key-{i}' for i in range(record_idx, record_idx + count)]
            record_idx += count
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify total records
        assert sum(record_counts.values()) == total_records
        
        # Verify partition count
        assert len(record_counts) == partition_count
        
        # Verify logging
        mock_logging.info.assert_any_call("Total records: %d", total_records)
        mock_logging.info.assert_any_call("Number of partitions: %d", partition_count)

    def test_return_types(self, test_instance, mock_logging):
        """Test that return types are correct."""
        partition_mapping = {
            0: ['user-1', 'order-1'],
            1: ['product-1']
        }
        
        result = test_instance._KeyDistributionAnalyzer__analyze_distribution(partition_mapping)
        
        # Verify return is a tuple
        assert isinstance(result, tuple)
        assert len(result) == 2
        
        # Verify first element (record counts)
        record_counts = result[0]
        assert isinstance(record_counts, dict)
        for key, value in record_counts.items():
            assert isinstance(key, int)
            assert isinstance(value, int)
        
        # Verify second element (key patterns)
        key_patterns = result[1]
        assert isinstance(key_patterns, (dict, defaultdict))
        for pattern, partitions in key_patterns.items():
            assert isinstance(pattern, str)
            assert isinstance(partitions, (dict, defaultdict))
            for partition, count in partitions.items():
                assert isinstance(partition, int)
                assert isinstance(count, int)

    def test_single_key_per_partition(self, test_instance, mock_logging):
        """Test distribution with exactly one key per partition."""
        partition_mapping = {
            0: ['key-0'],
            1: ['key-1'],
            2: ['key-2'],
            3: ['key-3']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify each partition has 1 record (25%)
        for partition in range(4):
            assert record_counts[partition] == 1
            mock_logging.info.assert_any_call(
                "Partition %d: %d records (%.1f%%)", partition, 1, 25.0
            )

    def test_unbalanced_distribution(self, test_instance, mock_logging):
        """Test highly unbalanced distribution across partitions."""
        partition_mapping = {
            0: ['key-' + str(i) for i in range(90)],  # 90%
            1: ['key-' + str(i) for i in range(5)],   # 5%
            2: ['key-' + str(i) for i in range(5)]    # 5%
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify unbalanced counts
        assert record_counts[0] == 90
        assert record_counts[1] == 5
        assert record_counts[2] == 5
        
        # Verify percentages logged correctly
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 0, 90, 90.0
        )
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 1, 5, 5.0
        )
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 2, 5, 5.0
        )

    def test_keys_without_hyphen(self, test_instance, mock_logging):
        """Test behavior with keys that don't contain hyphens."""
        partition_mapping = {
            0: ['simplekey1', 'simplekey2'],
            1: ['anotherkey']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # When split on '-', keys without hyphen return whole string + '-'
        # split('-')[0] + '-' for 'simplekey1' = 'simplekey1-'
        assert 'simplekey1-' in key_patterns
        assert 'simplekey2-' in key_patterns
        assert 'anotherkey-' in key_patterns

    def test_large_partition_numbers(self, test_instance, mock_logging):
        """Test distribution with non-sequential, large partition numbers."""
        partition_mapping = {
            100: ['key-1', 'key-2'],
            205: ['key-3'],
            999: ['key-4', 'key-5', 'key-6']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify record counts for large partition numbers
        assert record_counts[100] == 2
        assert record_counts[205] == 1
        assert record_counts[999] == 3
        
        # Verify partitions are logged in sorted order
        mock_logging.info.assert_any_call(
            "Partition %d: %d records (%.1f%%)", 100, 2, pytest.approx(33.3, rel=0.1)
        )

    def test_defaultdict_behavior(self, test_instance, mock_logging):
        """Test that key_patterns uses defaultdict correctly."""
        partition_mapping = {
            0: ['pattern1-key1', 'pattern2-key1'],
            1: ['pattern1-key2', 'pattern3-key1']
        }
        
        record_counts, key_patterns = test_instance._KeyDistributionAnalyzer__analyze_distribution(
            partition_mapping
        )
        
        # Verify defaultdict structure
        assert 'pattern1-' in key_patterns
        assert 'pattern2-' in key_patterns
        assert 'pattern3-' in key_patterns
        
        # Verify counts
        assert key_patterns['pattern1-'][0] == 1
        assert key_patterns['pattern1-'][1] == 1
        assert key_patterns['pattern2-'][0] == 1
        assert key_patterns['pattern3-'][1] == 1
        
        # pattern2- and pattern3- should not have entries for certain partitions
        assert 1 not in key_patterns['pattern2-']
        assert 0 not in key_patterns['pattern3-']
