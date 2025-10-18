import logging
import pytest
from unittest.mock import Mock, MagicMock, patch, call
import json

from src.key_distribution_analyzer import KeySimulationType


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
def mock_producer():
    """Fixture for mocked Kafka Producer."""
    with patch('src.key_distribution_analyzer.Producer') as mock_producer_class:
        producer_instance = MagicMock()
        mock_producer_class.return_value = producer_instance
        yield producer_instance


@pytest.fixture
def mock_serializer():
    """Fixture for mocked StringSerializer."""
    with patch('src.key_distribution_analyzer.StringSerializer') as mock_serializer_class:
        serializer_instance = MagicMock()
        serializer_instance.return_value = b'serialized_key'
        mock_serializer_class.return_value = serializer_instance
        yield serializer_instance


@pytest.fixture
def mock_time():
    """Fixture for mocked time.time()."""
    with patch('src.key_distribution_analyzer.time.time') as mock_time_func:
        mock_time_func.return_value = 1234567890.0
        yield mock_time_func


@pytest.fixture
def mock_logging():
    """Fixture for mocked logging."""
    with patch('src.key_distribution_analyzer.logging') as mock_log:
        yield mock_log


@pytest.fixture
def test_instance(mock_kafka_config):
    """Fixture for test class instance."""
    from src.key_distribution_analyzer import KeyDistributionAnalyzer
    instance = KeyDistributionAnalyzer("lkc-abc1234", "localhost:9092", "api_key", "api_secret")
    instance.kafka_producer_config = mock_kafka_config
    return instance


class TestProduceTestRecords:
    """Test suite for KeyDistributionAnalyzer.__produce_test_records method."""

    def test_moderate_repetition_key_pattern(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test MODERATE_REPETITION key simulation type."""
        topic = "test-topic"
        record_count = 250
        key_patterns = ["user-", "order-", "product-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.MODERATE_REPETITION
        )
        
        # Verify produce was called correct number of times
        assert mock_producer.produce.call_count == record_count
        
        # Verify key pattern cycles correctly (modulo 100)
        calls = mock_producer.produce.call_args_list
        
        # Test specific records
        test_indices = [0, 1, 2, 100, 101, 249]
        for i in test_indices:
            call_kwargs = calls[i][1]
            expected_pattern = key_patterns[i % len(key_patterns)]
            expected_key = f"{expected_pattern}{i % 100}"
            
            # Verify the value contains correct key
            value_dict = json.loads(call_kwargs['value'].decode('utf-8'))
            assert value_dict['key'] == expected_key
            assert value_dict['id'] == i
            assert value_dict['timestamp'] == 1234567890.0
            assert value_dict['data'] == f"test_record_{i}"
        
        # Verify flush was called
        mock_producer.flush.assert_called_once()
        
        # Verify logging
        mock_logging.info.assert_called_with("Producing %d records...", record_count)

    def test_less_repetition_key_pattern(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test LESS_REPETITION key simulation type."""
        topic = "test-topic"
        record_count = 1500
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.LESS_REPETITION
        )
        
        # Verify key uses modulo 1000
        calls = mock_producer.produce.call_args_list
        test_indices = [0, 50, 999, 1000, 1499]
        
        for i in test_indices:
            value_dict = json.loads(calls[i][1]['value'].decode('utf-8'))
            expected_key = f"key-{i % 1000}"
            assert value_dict['key'] == expected_key
            assert value_dict['id'] == i

    def test_more_repetition_key_pattern(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test MORE_REPETITION key simulation type."""
        topic = "test-topic"
        record_count = 50
        key_patterns = ["repeat-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.MORE_REPETITION
        )
        
        # Verify key uses modulo 10
        calls = mock_producer.produce.call_args_list
        test_indices = [0, 9, 10, 20, 35, 49]
        
        for i in test_indices:
            value_dict = json.loads(calls[i][1]['value'].decode('utf-8'))
            expected_key = f"repeat-{i % 10}"
            assert value_dict['key'] == expected_key

    def test_no_repetition_key_pattern(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test NO_REPETITION key simulation type."""
        topic = "test-topic"
        record_count = 100
        key_patterns = ["unique-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify each key is unique (no modulo applied)
        calls = mock_producer.produce.call_args_list
        keys_seen = set()
        
        for i in range(record_count):
            value_dict = json.loads(calls[i][1]['value'].decode('utf-8'))
            expected_key = f"unique-{i}"
            assert value_dict['key'] == expected_key
            assert expected_key not in keys_seen
            keys_seen.add(expected_key)
        
        # Verify all keys are unique
        assert len(keys_seen) == record_count

    def test_hot_key_data_skew(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test HOT_KEY_DATA_SKEW key simulation type."""
        topic = "test-topic"
        record_count = 100
        key_patterns = ["ignored"]  # Should be ignored for this type
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.HOT_KEY_DATA_SKEW
        )
        
        # Verify 80% use "hot-key" and 20% use "cold-key-{id}"
        calls = mock_producer.produce.call_args_list
        hot_key_count = 0
        cold_key_count = 0
        
        for i in range(record_count):
            value_dict = json.loads(calls[i][1]['value'].decode('utf-8'))
            if i < int(record_count * 0.8):
                assert value_dict['key'] == "hot-key"
                hot_key_count += 1
            else:
                assert value_dict['key'] == f"cold-key-{i}"
                cold_key_count += 1
        
        assert hot_key_count == 80
        assert cold_key_count == 20

    @pytest.mark.parametrize("record_count,expected_hot_count", [
        (100, 80),
        (50, 40),
        (1000, 800),
        (10, 8)
    ])
    def test_hot_key_data_skew_various_counts(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging,
        record_count, expected_hot_count
    ):
        """Test HOT_KEY_DATA_SKEW with various record counts."""
        topic = "test-topic"
        key_patterns = ["ignored"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.HOT_KEY_DATA_SKEW
        )
        
        calls = mock_producer.produce.call_args_list
        hot_key_count = sum(
            1 for i in range(record_count)
            if json.loads(calls[i][1]['value'].decode('utf-8'))['key'] == "hot-key"
        )
        
        assert hot_key_count == expected_hot_count

    def test_buffer_error_handling(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test BufferError exception handling and retry logic."""
        # Make produce raise BufferError on first call, succeed on second
        mock_producer.produce.side_effect = [
            BufferError("Queue full"),
            None,
            None
        ]
        mock_producer.__len__ = Mock(return_value=1000)
        
        topic = "test-topic"
        record_count = 2
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify warning was logged
        mock_logging.warning.assert_called()
        warning_call = mock_logging.warning.call_args[0]
        assert "Local producer queue is full" in warning_call[0]
        
        # Verify poll was called with timeout 1 after BufferError
        poll_calls = mock_producer.poll.call_args_list
        assert call(1) in poll_calls
        
        # Verify produce was called 3 times (initial + retry + second record)
        assert mock_producer.produce.call_count == 3

    def test_general_exception_handling(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test general exception handling during produce."""
        # Make produce raise a general exception on first call
        test_exception = Exception("Test error")
        mock_producer.produce.side_effect = [test_exception, None]
        
        topic = "test-topic"
        record_count = 2
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify error was logged
        mock_logging.error.assert_called_once()
        error_call = mock_logging.error.call_args[0]
        assert "Error producing record" in error_call[0]
        assert error_call[1] == 0
        assert error_call[2] == test_exception
        
        # Verify flush was still called
        mock_producer.flush.assert_called_once()

    def test_serializer_initialization(
        self, test_instance, mock_serializer, mock_time, mock_logging
    ):
        """Test StringSerializer is initialized with correct encoding."""
        with patch('src.key_distribution_analyzer.StringSerializer') as mock_serializer_class:
            mock_serializer_class.return_value = MagicMock(return_value=b'key')
            
            test_instance._KeyDistributionAnalyzer__produce_test_records(
                "test-topic", 1, ["key-"], KeySimulationType.NO_REPETITION
            )
            
            # Verify StringSerializer was initialized with 'utf_8'
            mock_serializer_class.assert_called_once_with('utf_8')

    def test_value_structure(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test the structure of produced values."""
        topic = "test-topic"
        record_count = 5
        key_patterns = ["test-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify structure for all produced records
        calls = mock_producer.produce.call_args_list
        
        for i in range(record_count):
            call_kwargs = calls[i][1]
            value_dict = json.loads(call_kwargs['value'].decode('utf-8'))
            
            # Verify value structure
            assert 'id' in value_dict
            assert 'key' in value_dict
            assert 'timestamp' in value_dict
            assert 'data' in value_dict
            
            assert value_dict['id'] == i
            assert value_dict['key'] == f'test-{i}'
            assert value_dict['timestamp'] == 1234567890.0
            assert value_dict['data'] == f'test_record_{i}'
            
            # Verify topic and key parameters
            assert call_kwargs['topic'] == topic
            assert call_kwargs['key'] == b'serialized_key'

    def test_poll_called_during_production(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test that poll(0) is called during production."""
        topic = "test-topic"
        record_count = 10
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify poll(0) was called for each record
        poll_zero_calls = [call for call in mock_producer.poll.call_args_list if call == call(0)]
        assert len(poll_zero_calls) == record_count

    def test_delivery_callback_passed(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test that delivery callback is passed to producer."""
        topic = "test-topic"
        record_count = 1
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # Verify on_delivery callback was passed
        call_kwargs = mock_producer.produce.call_args[1]
        assert 'on_delivery' in call_kwargs
        assert call_kwargs['on_delivery'] == test_instance._KeyDistributionAnalyzer__delivery_callback

    @pytest.mark.parametrize("simulation_type,key_patterns,record_id,expected_suffix", [
        (KeySimulationType.MODERATE_REPETITION, ["prefix-"], 0, "0"),
        (KeySimulationType.MODERATE_REPETITION, ["prefix-"], 150, "50"),
        (KeySimulationType.LESS_REPETITION, ["prefix-"], 0, "0"),
        (KeySimulationType.LESS_REPETITION, ["prefix-"], 1500, "500"),
        (KeySimulationType.MORE_REPETITION, ["prefix-"], 0, "0"),
        (KeySimulationType.MORE_REPETITION, ["prefix-"], 25, "5"),
        (KeySimulationType.NO_REPETITION, ["prefix-"], 0, "0"),
        (KeySimulationType.NO_REPETITION, ["prefix-"], 12345, "12345"),
    ])
    def test_key_generation_patterns(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging,
        simulation_type, key_patterns, record_id, expected_suffix
    ):
        """Test key generation for different simulation types and record IDs."""
        # Produce enough records to reach the test record_id
        record_count = record_id + 1
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            "test-topic", record_count, key_patterns, simulation_type
        )
        
        # Get the specific call for record_id
        call_kwargs = mock_producer.produce.call_args_list[record_id][1]
        value_dict = json.loads(call_kwargs['value'].decode('utf-8'))
        
        expected_key = f"prefix-{expected_suffix}"
        assert value_dict['key'] == expected_key

    def test_multiple_key_patterns_cycling(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test that multiple key patterns cycle correctly."""
        topic = "test-topic"
        record_count = 12
        key_patterns = ["user-", "order-", "product-", "payment-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        calls = mock_producer.produce.call_args_list
        
        for i in range(record_count):
            value_dict = json.loads(calls[i][1]['value'].decode('utf-8'))
            expected_pattern = key_patterns[i % len(key_patterns)]
            assert value_dict['key'].startswith(expected_pattern)

    def test_empty_record_count(
        self, test_instance, mock_producer, mock_serializer, mock_time, mock_logging
    ):
        """Test behavior with zero records."""
        topic = "test-topic"
        record_count = 0
        key_patterns = ["key-"]
        
        test_instance._KeyDistributionAnalyzer__produce_test_records(
            topic, record_count, key_patterns, KeySimulationType.NO_REPETITION
        )
        
        # No produce calls should be made
        assert mock_producer.produce.call_count == 0
        
        # Flush should still be called
        mock_producer.flush.assert_called_once()
