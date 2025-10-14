import json
import logging
from dotenv import load_dotenv
import os
import pytest

from cc_clients_python_lib.environment_client import EnvironmentClient
from cc_clients_python_lib.iam_client import IamClient
from cc_clients_python_lib.http_status import HttpStatus


__copyright__  = "Copyright (c) 2025 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings (J3)"]
__maintainer__ = "Jeffrey Jonathan Jennings (J3)"
__email__      = "j3@thej3.com"
__status__     = "dev"
 

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def kafka_cluster_id():
    """Load the Test Kafka cluster ID from the IAM variables."""
    load_dotenv()
    return os.getenv("TEST_KAFKA_CLUSTER_ID")

@pytest.fixture
def principal_id():
    """Load the Test Principal ID from the IAM variables."""
    load_dotenv()
    return os.getenv("PRINCIPAL_ID")
 
@pytest.fixture
def iam_client():
    """Load the Confluent Cloud credentials from the IAM variables."""
    load_dotenv()
    iam_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    yield IamClient(iam_config)

@pytest.fixture
def environment_client():
    """Load the Confluent Cloud credentials from the IAM variables."""
    load_dotenv()
    environment_config = json.loads(os.getenv("CONFLUENT_CLOUD_CREDENTIAL", "{}"))
    yield EnvironmentClient(environment_config)


class TestIamClient:
    """Test Suite for the IamClient class."""

    def test_get_all_api_keys_by_principal_id(self, iam_client, principal_id):
        """Test the get_all_api_keys_by_principal_id() function."""
    
        http_status_code, error_message, api_keys = iam_client.get_all_api_keys_by_principal_id(principal_id=principal_id)
    
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("API Keys: %d", len(api_keys))

            beautified = json.dumps(api_keys, indent=4, sort_keys=True)
            logger.info("API Keys: %s", beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, API Keys: %s", http_status_code, error_message, api_keys)
            return
        
    def test_delete_all_api_keys_by_principal_id(self, iam_client, principal_id):
        """Test the delete_api_key() function by deleting all API keys for the given Principal ID."""
    
        http_status_code, error_message, api_keys = iam_client.get_all_api_keys_by_principal_id(principal_id=principal_id)
    
        try:
            assert http_status_code == HttpStatus.OK, f"HTTP Status Code: {http_status_code}"

            logger.info("API Keys: %d", len(api_keys))

            beautified = json.dumps(api_keys, indent=4, sort_keys=True)
            logger.info("API Keys: %s", beautified)
        except AssertionError as e:
            logger.error(e)
            logger.error("HTTP Status Code: %d, Error Message: %s, API Keys: %s", http_status_code, error_message, api_keys)
            return

        for index, api_key in enumerate(api_keys.values()):
            http_status_code, error_message = iam_client.delete_api_key(api_key=api_key["api_key"])
    
            try:
                assert http_status_code == HttpStatus.NO_CONTENT, f"HTTP Status Code: {http_status_code}"

                logger.info("%d of %d Successfully deleted API Key: %s", index + 1, len(api_keys), api_key['api_key'])
            except AssertionError as e:
                logger.error(e)
                logger.error("HTTP Status Code: %d, Error Message: %s", http_status_code, error_message)
                return
