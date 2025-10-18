import pytest
from unittest.mock import  patch
import logging


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

@pytest.fixture
def sample_keys():
    """Fixture for sample keys."""
    return ['user-1', 'user-2', 'order-1', 'order-2', 'product-1']

@pytest.fixture
def large_key_set():
    """Fixture for a large set of keys."""
    return [f'key-{i}' for i in range(1000)]


class TestPartitionStrategies:
    """Test suite for KeyDistributionAnalyzer partition strategy methods."""

    # ==================== __test_partition_strategies ====================
    
    def test_partition_strategies_all_strategies_called(
        self, test_instance, mock_logging, sample_keys
    ):
        """Test that all partition strategies are called and results returned."""
        partition_count = 3
        
        results = test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            sample_keys, partition_count
        )
        
        # Verify all strategies are in results
        expected_strategies = [
            'default_(murmur_hash_2)',
            'round_robin',
            'sticky',
            'range_based_custom',
            'custom'
        ]
        
        assert len(results) == len(expected_strategies)
        for strategy in expected_strategies:
            assert strategy in results
            assert isinstance(results[strategy], dict)
        
        # Verify logging header
        mock_logging.info.assert_any_call("=== Partition Strategy Comparison ===")

    def test_partition_strategies_distribution_logging(
        self, test_instance, mock_logging, sample_keys
    ):
        """Test that distribution results are logged for each strategy."""
        partition_count = 2
        
        test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            sample_keys, partition_count
        )
        
        # Verify strategy headers are logged
        strategy_headers = [
            call for call in mock_logging.info.call_args_list
            if 'Strategy:' in str(call[0])
        ]
        
        # Should have 5 strategy headers
        assert len(strategy_headers) == 5

    def test_partition_strategies_cv_calculation(
        self, test_instance, mock_logging, large_key_set
    ):
        """Test that CV (Coefficient of Variation) is calculated and logged."""
        partition_count = 4
        
        test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            large_key_set, partition_count
        )
        
        # Verify CV logging
        cv_calls = [
            call for call in mock_logging.info.call_args_list
            if len(call[0]) > 0 and 'CV:' in str(call[0][0])
        ]
        
        # Should have CV for each strategy (5 strategies)
        assert len(cv_calls) == 5

    def test_partition_strategies_return_structure(
        self, test_instance, mock_logging, sample_keys
    ):
        """Test the structure of returned results."""
        partition_count = 3
        
        results = test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            sample_keys, partition_count
        )
        
        # Verify structure
        for strategy_name, distribution in results.items():
            assert isinstance(strategy_name, str)
            assert isinstance(distribution, dict)
            
            # Verify all values are integers
            for partition, count in distribution.items():
                assert isinstance(partition, int)
                assert isinstance(count, int)
                assert partition >= 0
                assert partition < partition_count

    def test_partition_strategies_empty_keys(
        self, test_instance, mock_logging
    ):
        """Test behavior with empty key list."""
        empty_keys = []
        partition_count = 3
        
        results = test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            empty_keys, partition_count
        )
        
        # All strategies should return empty or zero distributions
        for strategy_name, distribution in results.items():
            total_keys = sum(distribution.values())
            assert total_keys == 0

    # ==================== __test_hash_distribution ====================
    
    def test_hash_distribution_basic(
        self, test_instance, mock_logging, sample_keys
    ):
        """Test basic hash distribution functionality."""
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__test_hash_distribution(
            sample_keys, partition_count
        )
        
        # Verify distribution exists
        assert distribution is not None
        assert isinstance(distribution, dict)
        
        # Verify total keys match
        total_keys = sum(distribution.values())
        assert total_keys == len(sample_keys)
        
        # Verify logging
        mock_logging.info.assert_any_call("=== Hash Function Distribution Test ===")
        mock_logging.info.assert_any_call("Theoretical hash distribution:")

    def test_hash_distribution_logging(
        self, test_instance, mock_logging, sample_keys
    ):
        """Test that hash distribution results are logged correctly."""
        partition_count = 2
        
        test_instance._KeyDistributionAnalyzer__test_hash_distribution(
            sample_keys, partition_count
        )
        
        # Verify partition distribution is logged
        partition_calls = [
            call for call in mock_logging.info.call_args_list
            if len(call[0]) > 2 and call[0][0] == "Partition %d: %d keys (%.1f%%)"
        ]
        
        assert len(partition_calls) > 0

    # ==================== __murmur2_hash_strategy ====================
    
    def test_murmur2_hash_strategy_basic(self, test_instance):
        """Test basic MurmurHash2 strategy."""
        keys = ['key-1', 'key-2', 'key-3', 'key-4', 'key-5']
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__murmur2_hash_strategy(
            keys, partition_count
        )
        
        # Verify distribution
        assert isinstance(distribution, dict)
        total_keys = sum(distribution.values())
        assert total_keys == len(keys)
        
        # Verify all partitions are within range
        for partition in distribution.keys():
            assert 0 <= partition < partition_count

    def test_murmur2_hash_strategy_deterministic(self, test_instance):
        """Test that MurmurHash2 is deterministic."""
        keys = ['test-key-1', 'test-key-2', 'test-key-3']
        partition_count = 3
        
        # Run twice
        distribution1 = test_instance._KeyDistributionAnalyzer__murmur2_hash_strategy(
            keys, partition_count
        )
        distribution2 = test_instance._KeyDistributionAnalyzer__murmur2_hash_strategy(
            keys, partition_count
        )
        
        # Should be identical
        assert distribution1 == distribution2

    def test_murmur2_hash_strategy_same_key_same_partition(self, test_instance):
        """Test that same key always goes to same partition."""
        keys = ['same-key'] * 10
        partition_count = 5
        
        distribution = test_instance._KeyDistributionAnalyzer__murmur2_hash_strategy(
            keys, partition_count
        )
        
        # All keys should go to same partition
        assert len(distribution) == 1
        assert list(distribution.values())[0] == 10

    def test_murmur2_hash_strategy_large_dataset(self, test_instance, large_key_set):
        """Test MurmurHash2 with large dataset for distribution quality."""
        partition_count = 10
        
        distribution = test_instance._KeyDistributionAnalyzer__murmur2_hash_strategy(
            large_key_set, partition_count
        )
        
        # Verify all partitions have some keys (probabilistic)
        assert len(distribution) == partition_count
        
        # Check distribution is reasonably balanced
        counts = list(distribution.values())
        avg = sum(counts) / len(counts)
        
        # Each partition should be within 50% of average (loose check)
        for count in counts:
            assert count > avg * 0.5
            assert count < avg * 1.5

    # ==================== __murmur2_hash ====================
    
    def test_murmur2_hash_basic(self, test_instance):
        """Test basic MurmurHash2 computation."""
        key = b'test-key'
        
        hash_value = test_instance._KeyDistributionAnalyzer__murmur2_hash(key)
        
        # Verify hash is an integer
        assert isinstance(hash_value, int)
        
        # Verify hash is deterministic
        hash_value2 = test_instance._KeyDistributionAnalyzer__murmur2_hash(key)
        assert hash_value == hash_value2

    def test_murmur2_hash_different_keys(self, test_instance):
        """Test that different keys produce different hashes."""
        key1 = b'key-1'
        key2 = b'key-2'
        
        hash1 = test_instance._KeyDistributionAnalyzer__murmur2_hash(key1)
        hash2 = test_instance._KeyDistributionAnalyzer__murmur2_hash(key2)
        
        # Different keys should (almost always) produce different hashes
        assert hash1 != hash2

    def test_murmur2_hash_empty_key(self, test_instance):
        """Test MurmurHash2 with empty key."""
        key = b''
        
        hash_value = test_instance._KeyDistributionAnalyzer__murmur2_hash(key)
        
        assert isinstance(hash_value, int)

    # ==================== __round_robin_strategy ====================
    
    def test_round_robin_strategy_even_distribution(self, test_instance):
        """Test round-robin creates even distribution."""
        keys = [f'key-{i}' for i in range(30)]
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__round_robin_strategy(
            keys, partition_count
        )
        
        # Each partition should have exactly 10 keys
        assert distribution[0] == 10
        assert distribution[1] == 10
        assert distribution[2] == 10

    def test_round_robin_strategy_ignores_keys(self, test_instance):
        """Test that round-robin ignores key values."""
        keys1 = ['key-a', 'key-b', 'key-c', 'key-d']
        keys2 = ['key-x', 'key-y', 'key-z', 'key-w']
        partition_count = 2
        
        distribution1 = test_instance._KeyDistributionAnalyzer__round_robin_strategy(
            keys1, partition_count
        )
        distribution2 = test_instance._KeyDistributionAnalyzer__round_robin_strategy(
            keys2, partition_count
        )
        
        # Same distribution regardless of key values
        assert distribution1 == distribution2

    def test_round_robin_strategy_uneven_keys(self, test_instance):
        """Test round-robin with number of keys not divisible by partition count."""
        keys = [f'key-{i}' for i in range(10)]
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__round_robin_strategy(
            keys, partition_count
        )
        
        # 10 keys, 3 partitions: should be 4, 3, 3 or similar
        assert sum(distribution.values()) == 10
        assert len(distribution) == 3

    # ==================== __sticky_strategy ====================
    
    @patch('src.key_distribution_analyzer.random.randint')
    @patch('src.key_distribution_analyzer.random.choice')
    def test_sticky_strategy_basic(
        self, mock_choice, mock_randint, test_instance
    ):
        """Test basic sticky partitioner behavior."""
        mock_randint.return_value = 0  # Start at partition 0
        mock_choice.return_value = 1   # Switch to partition 1
        
        keys = [f'key-{i}' for i in range(150)]
        partition_count = 3
        batch_size = 100
        
        distribution = test_instance._KeyDistributionAnalyzer__sticky_strategy(
            keys, partition_count, batch_size=batch_size
        )
        
        # Should stick to partition 0 for first 100, then switch
        assert distribution[0] == 100
        assert sum(distribution.values()) == 150

    @patch('src.key_distribution_analyzer.random.randint')
    def test_sticky_strategy_single_batch(
        self, mock_randint, test_instance
    ):
        """Test sticky strategy with keys fitting in single batch."""
        mock_randint.return_value = 2
        
        keys = [f'key-{i}' for i in range(50)]
        partition_count = 4
        batch_size = 100
        
        distribution = test_instance._KeyDistributionAnalyzer__sticky_strategy(
            keys, partition_count, batch_size=batch_size
        )
        
        # All should go to partition 2
        assert distribution[2] == 50
        assert len(distribution) == 1

    def test_sticky_strategy_batch_switching(self, test_instance):
        """Test that sticky strategy switches partitions at batch boundaries."""
        keys = [f'key-{i}' for i in range(250)]
        partition_count = 5
        batch_size = 100
        
        distribution = test_instance._KeyDistributionAnalyzer__sticky_strategy(
            keys, partition_count, batch_size=batch_size
        )
        
        # Should have switched partitions at least once
        assert len(distribution) >= 2
        assert sum(distribution.values()) == 250

    # ==================== __select_next_partition_kafka4 ====================

    @patch('src.key_distribution_analyzer.random.choice')
    def test_select_next_partition_avoids_current(
        self, mock_choice, test_instance
    ):
        """Test that next partition selection avoids current partition."""
        mock_choice.return_value = 2
        
        current_partition = 1
        partition_count = 5
        
        next_partition = test_instance._KeyDistributionAnalyzer__select_next_partition_kafka4(
            current_partition, partition_count
        )
        
        # Verify the returned partition is not the current one
        assert next_partition != current_partition
        assert next_partition == 2
        
        # Should call choice with partitions excluding current
        called_with = mock_choice.call_args[0][0]
        assert current_partition not in called_with
        assert len(called_with) == partition_count - 1
        assert set(called_with) == {0, 2, 3, 4}  # All except partition 1

    def test_select_next_partition_single_partition(self, test_instance):
        """Test next partition selection with only one partition."""
        current_partition = 0
        partition_count = 1
        
        next_partition = test_instance._KeyDistributionAnalyzer__select_next_partition_kafka4(
            current_partition, partition_count
        )
        
        # Should return 0 (only option)
        assert next_partition == 0

    @patch('src.key_distribution_analyzer.random.choice')
    def test_select_next_partition_two_partitions(
        self, mock_choice, test_instance
    ):
        """Test next partition selection with two partitions."""
        mock_choice.return_value = 1
        
        current_partition = 0
        partition_count = 2
        
        next_partition = test_instance._KeyDistributionAnalyzer__select_next_partition_kafka4(
            current_partition, partition_count
        )
        
        # Should return the other partition
        assert next_partition == 1
        assert next_partition != current_partition
        
        # Verify available partitions list
        called_with = mock_choice.call_args[0][0]
        assert called_with == [1]

    @patch('src.key_distribution_analyzer.random.choice')
    def test_select_next_partition_returns_valid_partition(
        self, mock_choice, test_instance
    ):
        """Test that returned partition is always within valid range."""
        mock_choice.return_value = 7
        
        current_partition = 3
        partition_count = 10
        
        next_partition = test_instance._KeyDistributionAnalyzer__select_next_partition_kafka4(
            current_partition, partition_count
        )
        
        # Verify returned partition is valid
        assert 0 <= next_partition < partition_count
        assert next_partition == 7
        assert next_partition != current_partition

    def test_select_next_partition_available_list(self, test_instance):
        """Test that available partitions list is correct."""
        with patch('src.key_distribution_analyzer.random.choice') as mock_choice:
            current_partition = 5
            partition_count = 8
            
            test_instance._KeyDistributionAnalyzer__select_next_partition_kafka4(
                current_partition, partition_count
            )
            
            # Get the list passed to random.choice
            called_with = mock_choice.call_args[0][0]
            
            # Should contain all partitions except current
            expected_partitions = [0, 1, 2, 3, 4, 6, 7]
            assert sorted(called_with) == expected_partitions
            assert 5 not in called_with

    # ==================== __range_based_customer_strategy ====================
    
    def test_range_based_strategy_basic(self, test_instance):
        """Test basic range-based partitioning."""
        keys = ['key-1', 'key-2', 'key-3', 'key-4', 'key-5', 'key-6']
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__range_based_customer_strategy(
            keys, partition_count
        )
        
        # Verify all keys are distributed
        assert sum(distribution.values()) == len(keys)
        
        # Verify partitions are used
        assert len(distribution) <= partition_count

    def test_range_based_strategy_groups_similar_keys(self, test_instance):
        """Test that range-based strategy groups similar keys."""
        keys = ['a-key', 'a-key', 'b-key', 'b-key', 'z-key', 'z-key']
        partition_count = 2
        
        distribution = test_instance._KeyDistributionAnalyzer__range_based_customer_strategy(
            keys, partition_count
        )
        
        # Similar keys should be grouped
        # After sorting unique: ['a-key', 'b-key', 'z-key']
        # Should be distributed based on sorted order
        assert sum(distribution.values()) == 6

    def test_range_based_strategy_duplicate_keys(self, test_instance):
        """Test range-based strategy with duplicate keys."""
        keys = ['key-5'] * 10 + ['key-1'] * 5
        partition_count = 3
        
        distribution = test_instance._KeyDistributionAnalyzer__range_based_customer_strategy(
            keys, partition_count
        )
        
        # All instances of same key should go to same partition
        assert sum(distribution.values()) == 15

    # ==================== __custom_strategy ====================
    
    @patch('src.key_distribution_analyzer.mmh3.hash')
    def test_custom_strategy_basic(self, mock_mmh3_hash, test_instance):
        """Test basic custom strategy using mmh3."""
        mock_mmh3_hash.side_effect = [100, 200, 300]
        
        keys = ['key-1', 'key-2', 'key-3']
        partition_count = 5
        
        distribution = test_instance._KeyDistributionAnalyzer__custom_strategy(
            keys, partition_count
        )
        
        # Verify mmh3.hash was called
        assert mock_mmh3_hash.call_count == 3
        
        # Verify distribution
        assert sum(distribution.values()) == 3

    @patch('src.key_distribution_analyzer.mmh3.hash')
    def test_custom_strategy_partition_calculation(
        self, mock_mmh3_hash, test_instance
    ):
        """Test custom strategy partition calculation."""
        # Hash values and expected partitions for 5 partitions
        mock_mmh3_hash.side_effect = [10, 23, 47]  # 10%5=0, 23%5=3, 47%5=2
        
        keys = ['key-1', 'key-2', 'key-3']
        partition_count = 5
        
        distribution = test_instance._KeyDistributionAnalyzer__custom_strategy(
            keys, partition_count
        )
        
        # Verify partitions
        assert distribution[0] == 1
        assert distribution[3] == 1
        assert distribution[2] == 1

    @patch('src.key_distribution_analyzer.mmh3.hash')
    def test_custom_strategy_deterministic(
        self, mock_mmh3_hash, test_instance
    ):
        """Test that custom strategy is deterministic."""
        mock_mmh3_hash.side_effect = [100, 200, 100, 200]
        
        keys = ['key-1', 'key-2']
        partition_count = 3
        
        # Run twice
        distribution1 = test_instance._KeyDistributionAnalyzer__custom_strategy(
            keys, partition_count
        )
        
        mock_mmh3_hash.side_effect = [100, 200]
        distribution2 = test_instance._KeyDistributionAnalyzer__custom_strategy(
            keys, partition_count
        )
        
        # Should produce same results
        assert distribution1 == distribution2

    # ==================== Integration Tests ====================
    
    def test_all_strategies_handle_same_input(
        self, test_instance, mock_logging
    ):
        """Test that all strategies can handle the same input."""
        keys = [f'key-{i}' for i in range(100)]
        partition_count = 4
        
        results = test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            keys, partition_count
        )
        
        # All strategies should return distributions
        for strategy_name, distribution in results.items():
            assert sum(distribution.values()) == len(keys)
            for partition in distribution.keys():
                assert 0 <= partition < partition_count

    @pytest.mark.parametrize("partition_count", [1, 2, 5, 10, 20])
    def test_strategies_various_partition_counts(
        self, test_instance, mock_logging, partition_count
    ):
        """Test strategies with various partition counts."""
        keys = [f'key-{i}' for i in range(100)]
        
        results = test_instance._KeyDistributionAnalyzer__test_partition_strategies(
            keys, partition_count
        )
        
        for strategy_name, distribution in results.items():
            # Verify all keys are accounted for
            assert sum(distribution.values()) == len(keys)
            
            # Verify partitions are within range
            for partition in distribution.keys():
                assert 0 <= partition < partition_count

    def test_cv_quality_indicator_integration(
        self, test_instance, mock_logging
    ):
        """Test that CV quality indicator is called and logged."""
        keys = [f'key-{i}' for i in range(100)]
        partition_count = 4
        
        with patch.object(
            test_instance, '_KeyDistributionAnalyzer__cv_quality_indicator', return_value='Good'
        ) as mock_cv_indicator:
            test_instance._KeyDistributionAnalyzer__test_partition_strategies(
                keys, partition_count
            )
            
            # Should be called once per strategy (5 times)
            assert mock_cv_indicator.call_count == 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])