import tomllib
from pathlib import Path
import logging
import logging.config
from confluent_kafka.admin import ConfigResource, NewTopic

from constants import (DEFAULT_TOOL_LOG_FILE, 
                       DEFAULT_TOOL_LOG_FORMAT)


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def setup_logging(log_file: str = DEFAULT_TOOL_LOG_FILE) -> logging.Logger:
    """Load logging configuration from pyproject.toml.  If not found, use default logging.
    
    Arg(s):
        log_file (str): The log file name to use if no configuration is found.
        
    Return(s):
        logging.Logger: Configured logger instance.
    """
    pyproject_path = Path("pyproject.toml")
    
    if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
        
        # Extract logging config
        logging_config = config.get("tool", {}).get("logging", {})
        
        if logging_config:
            logging.config.dictConfig(logging_config)
        else:
            # Fallback to basic file logging
            logging.basicConfig(
                level=logging.INFO,
                format=DEFAULT_TOOL_LOG_FORMAT,
                filemode="w",  # This will reset the log file
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
    else:
        # Default logging setup if no pyproject.toml
        logging.basicConfig(
            level=logging.INFO,
            format=DEFAULT_TOOL_LOG_FORMAT,
            filemode="w",  # This will reset the log file
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    return logging.getLogger()

def create_topic_if_not_exists(self, topic_name: str, partition_count: int, replication_factor: int, data_retention_in_days: int) -> None:
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