# src/ateda_platform/orchestration/assets/hello_world_asset.py
from dagster import asset
import logging  # Use logging instead of print

logger = logging.getLogger(__name__)


@asset  # Basic asset definition
def hello_world_asset():
    """
    A simple asset that logs a greeting.
    """
    message = "Hello, Dagster! Greetings from ATEDA setup."
    logger.info(message)
    # Assets typically return a value or MetadataValue
    return message
