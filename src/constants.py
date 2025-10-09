from typing import Final


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


DEFAULT_USE_AWS_SECRETS_MANAGER: Final[str] = "False"

# Character repeat limit for string fields
DEFAULT_CHARACTER_REPEAT: Final[int] = 100

DEFAULT_USE_CONFLUENT_CLOUD_API_KEY_TO_FETCH_RESOURCE_CREDENTIALS: Final[str] = "False"

# Logging configuration
DEFAULT_TOOL_LOG_FILE: Final[str] = "kafka_cluster-topic-key-distribution-hot-partition-analyzer-tool.log"
DEFAULT_TOOL_LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

DEFAULT_TOPIC_CONSUMER_TIMEOUT_MS: Final[int] = 10000

DEFAULT_KAFKA_TOPIC_NAME: Final[str] = "_j3.key_distribution_test_topic"
DEFAULT_KAFKA_TOPIC_PARTITION_COUNT: Final[int] = 6
DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR: Final[int] = 3
DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS: Final[int] = 0  # 0 means infinite retention
DEFAULT_KAFKA_TOPIC_RECORD_COUNT: Final[int] = 1000
