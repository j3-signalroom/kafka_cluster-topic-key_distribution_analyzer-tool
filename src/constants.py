from typing import Final, List


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# AWS Secrets Manager configuration
DEFAULT_USE_AWS_SECRETS_MANAGER: Final[str] = "False"

# Logging configuration
DEFAULT_TOOL_LOG_FILE: Final[str] = "kafka_cluster-topic-key-distribution-analyzer-tool.log"
DEFAULT_TOOL_LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Kafka Topic configuration
DEFAULT_KAFKA_TOPIC_NAME: Final[str] = "_j3.key_distribution_test_topic"
DEFAULT_KAFKA_TOPIC_MINIMUM_PARTITION_COUNT: Final[int] = 6
DEFAULT_KAFKA_TOPIC_MAXIMUM_PARTITION_COUNT: Final[int] = 128
DEFAULT_KAFKA_TOPIC_REPLICATION_FACTOR: Final[int] = 3
DEFAULT_KAFKA_TOPIC_DATA_RETENTION_IN_DAYS: Final[int] = 0  # 0 means infinite retention
DEFAULT_KAFKA_TOPIC_KEY_PATTERN: Final[List] = ["tenant_id-", "user_id-"]