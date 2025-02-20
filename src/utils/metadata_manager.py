"""

"""

# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
import logging
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, errors

from typing import Dict, Any

# Set up logging
_logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - line: %(lineno)d",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Environment variables
load_dotenv(override=True)


class MetadataManager:
    """
    A class for managing metadata of a dataset and storing it in MongoDB.

    Attributes:
        df (pd.DataFrame): The dataset to be analyzed.
        db_name (str): The name of the MongoDB database.
        collection_name (str): The MongoDB collection to store metadata.
        client (MongoClient): MongoDB client instance.
        db (Database): MongoDB database instance.
        collection (Collection): MongoDB collection instance.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        dashboard_name: str,
        mongo_uri: str = "mongodb://localhost:27017",
        db_name: str = "DashboardMetadata",
    ) -> None:
        """
        Initializes the MetadataManager with the dataset and MongoDB configuration.

        Args:
            df: The dataset to be analyzed.
            dashboard_name: Unique name for the dashboard (acts as partition key).
            mongo_uri: MongoDB connection URI.
            db_name: Name of the MongoDB database.
        """
        self.df = df
        self.dashboard_name = dashboard_name  # Unique partition key
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db["metadata"]

        # Ensure the dashboard_name is unique using an index
        self.collection.create_index("dashboard_name", unique=True)

    def generate_metadata(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates metadata for the dataset.
        
        Args:
            summary (Dict): Data validation summary containing missing values and duplicates.

        Returns: 
            (Dict[str, Any]) - A dictionary containing metadata.
        """
        _logger.info("Generating metadata for dashboard: %s", self.dashboard_name)

        metadata_list = []
        for col in self.df.columns:
            sample_values = self.df[col].dropna().sample(5).tolist()
            metadata_list.append(
                {
                    "column_name": col,
                    "data_type": str(self.df[col].dtype),
                    "sample_values": sample_values,
                }
            )

        self.metadata = {
            "dashboard_name": self.dashboard_name,
            "columns": metadata_list,
            "data_summary": summary
        }
        _logger.info("Metadata generated.")
        return self.metadata

    def save_metadata(self) -> None:
        """
        Saves the metadata to MongoDB.
        """
        if not self.metadata:
            _logger.warning("No metadata to save.")
            return

        try:
            self.collection.insert_one(self.metadata)
            _logger.info("Metadata saved for dashboard: %s", self.dashboard_name)
        except errors.DuplicateKeyError:
            _logger.warning(
                "A dashboard with the name '%s' already exists. Use a different name.",
                self.dashboard_name,
            )

    def retrieve_metadata(self, dashboard_name: str) -> Dict[str, Any]:
        """
        Retrieves metadata from MongoDB for a given dashboard.

        Args:
            dashboard_name (str): Unique name of the dashboard.

        Returns:
            Dict[str, Any]: A dictionary containing metadata if found, or None.

        Raises:
            None
        """
        result = self.collection.find_one(
            {"dashboard_name": dashboard_name}, {"_id": 0, "columns": 1}
        )

        if not result:
            _logger.warning("No metadata found for dashboard: %s", dashboard_name)
            return None

        _logger.info("Metadata retrieved for dashboard: %s", dashboard_name)
        return result
