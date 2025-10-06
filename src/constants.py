from typing import Final


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Character repeat limit for string fields
DEFAULT_CHARACTER_REPEAT: Final[int] = 100

# Logging configuration
DEFAULT_TOOL_LOG_FILE: Final[str] = "kafka-cluster-topics-partition-count-recommender-tool.log"
DEFAULT_TOOL_LOG_FORMAT: Final[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Kafka topic configuration
DEFAULT_TOPIC_PARTITION_COUNT: Final[int] = 6
DEFAULT_TOPIC_REPLICATION_FACTOR: Final[int] = 3
DEFAULT_TOPIC_DATA_RETENTION_IN_DAYS: Final[int] = 0  # 0 means infinite retention
DEFAULT_TOPIC_RECORD_COUNT: Final[int] = 1000

DEFAULT_TOPIC_CONSUMER_TIMEOUT_MS: Final[int] = 10000
