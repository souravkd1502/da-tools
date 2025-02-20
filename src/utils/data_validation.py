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

class DataValidation:
    """
    A class for performing data validation on a Pandas DataFrame.
    
    The class provides methods to check for missing values, duplicate rows, 
    data type consistency, and other potential issues in the dataset.
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Initializes the DataValidation class with a Pandas DataFrame.

        :param df: The Pandas DataFrame to be validated.
        """
        self.df = df

    def check_missing_values(self) -> Dict[str, Any]:
        """
        Checks for missing values in each column of the DataFrame.

        :return: Dictionary containing missing value count and percentage per column.
        """
        missing_counts = self.df.isnull().sum()
        total_rows = len(self.df)
        missing_percentage = (missing_counts / total_rows * 100).round(2)
        
        missing_summary = {
            "missing_counts": missing_counts.to_dict(),
            "missing_percentage": missing_percentage.to_dict()
        }
        
        _logger.info("Missing values check completed.")
        return missing_summary
    
    def check_duplicates(self) -> int:
        """
        Checks for duplicate rows in the DataFrame.

        :return: The number of duplicate rows found.
        """
        duplicate_count = self.df.duplicated().sum()
        _logger.info("Duplicate rows check completed. Found %d duplicates.", duplicate_count)
        return int(duplicate_count)
    
    def check_data_types(self) -> Dict[str, str]:
        """
        Checks the data types of each column in the DataFrame.

        :return: Dictionary with column names as keys and data types as values.
        """
        data_types = self.df.dtypes.astype(str).to_dict()
        _logger.info("Data types check completed.")
        return data_types
    
    def check_outliers(self) -> Dict[str, Dict[str, float]]:
        """
        Identifies potential outliers in numerical columns using the IQR method.

        :return: Dictionary with column names as keys and number of outliers as values.
        """
        outlier_summary = {}
        
        for col in self.df.select_dtypes(include=['number']):
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outlier_count = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).sum()
            
            outlier_summary[col] = {
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "outlier_count": int(outlier_count)
            }
        
        _logger.info("Outlier detection completed.")
        return outlier_summary
    
    def check_unique_values(self) -> Dict[str, int]:
        """
        Checks the number of unique values in each column.

        :return: Dictionary with column names as keys and count of unique values as values.
        """
        unique_counts = self.df.nunique().to_dict()
        _logger.info("Unique values check completed.")
        return unique_counts
    
    @classmethod
    def generate_summary(cls, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates a summary report of data validation checks.

        :param df: The Pandas DataFrame to be validated.
        :return: A dictionary containing results of all validation checks.
        """
        validator = cls(df)
        summary = {
            "missing_values": validator.check_missing_values(),
            "duplicate_rows": validator.check_duplicates(),
            "data_types": validator.check_data_types(),
            "outliers": validator.check_outliers(),
            "unique_values": validator.check_unique_values()
        }
        
        _logger.info("Data validation summary generated.")
        return summary