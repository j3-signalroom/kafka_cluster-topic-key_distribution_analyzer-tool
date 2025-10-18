import tomllib
from pathlib import Path
import logging
import logging.config
from confluent_kafka.admin import NewTopic, AdminClient

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


def create_admin_client(bootstrap_server_uri: str, kafka_api_key: str, kafka_api_secret: str) -> AdminClient | None:
    """Create a Kafka AdminClient instance.

    Args:
        bootstrap_server_uri (str): Kafka bootstrap server URI.
        kafka_api_key (str): Kafka API key.
        kafka_api_secret (str): Kafka API secret.

    Returns:
        AdminClient | None: Configured Kafka AdminClient instance or None if creation failed.
    """

    try:
        # Instantiate the AdminClient with the provided credentials
        config = {
            'bootstrap.servers': bootstrap_server_uri,
            'security.protocol': "SASL_SSL",
            'sasl.mechanism': "PLAIN",
            'sasl.username': kafka_api_key,
            'sasl.password': kafka_api_secret,
        }
        return AdminClient(config)
    except Exception:
        return None


def create_topic_if_not_exists(admin_client, topic_name: str, partition_count: int, replication_factor: int, data_retention_in_days: int) -> bool:
    """Create the results topic if it doesn't exist.

    Args:
        admin_client: Kafka AdminClient instance.
        topic_name (str): Name of the Kafka topic.
        partition_count (int): Number of partitions for the topic.
        replication_factor (int): Replication factor for the topic.
        data_retention_in_days (int): Data retention period in days.
    
    Return(s):
        bool: True if the topic was created, False if it to create or recreate a Kafka
        topic.
    """
    if not delete_topic_if_exists(admin_client, topic_name):
        return False

    # Create new topic
    logging.info("Creating Kafka topic '%s' with %d partitions", topic_name, partition_count)

    new_topic = NewTopic(topic=topic_name,
                         num_partitions=partition_count,
                         replication_factor=replication_factor,
                         config={
                             'cleanup.policy': 'delete',
                             'retention.ms': '-1' if data_retention_in_days == 0 else str(data_retention_in_days * 24 * 60 * 60 * 1000),  # Convert days to milliseconds
                             'compression.type': 'lz4'
                         })
    
    futures = admin_client.create_topics([new_topic])
    
    # Wait for topic creation
    for topic, future in futures.items():
        try:
            future.result()  # Block until topic is created
            logging.info("Topic '%s' created successfully", topic)
        except Exception as e:
            logging.error("Failed to create topic '%s': %s", topic, e)
            return False
        
    return True
    

def delete_topic_if_exists(admin_client, topic_name: str) -> bool:
    """Delete the results topic if it exists.

    Args:
        admin_client: Kafka AdminClient instance.
        topic_name (str): Name of the Kafka topic.

    Returns:
        bool: True if the topic was deleted, False otherwise.
    """
    try:
        topic_list = admin_client.list_topics(timeout=10)
    except Exception as e:
        logging.error("Failed to list topics: %s", e)
        return False

    if topic_name in topic_list.topics:
        logging.info("Deleting Kafka topic '%s'.", topic_name)
        futures = admin_client.delete_topics([topic_name])

        for topic, future in futures.items():
            try:
                future.result()  # Block until topic is deleted
                logging.info("Topic '%s' deleted successfully", topic)
            except Exception as e:
                logging.error("Failed to delete topic '%s': %s", topic, e)
                return False
        return True
    else:
        logging.info("Kafka topic '%s' does not exist.", topic_name)
        return True