"""

"""

# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Set up logging
_logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - line: %(lineno)d",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Environment variables
load_dotenv(override=True)


class DataManager:
    def __init__(
        self,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
    ):
        """
        Initialize the DataManager with PostgreSQL connection parameters.

        Parameters:
            host (str): PostgreSQL host address
            database (str): Database name
            user (str): Database username
            password (str): Database password

        Attributes:
            host (str): PostgreSQL host address
            database (str): Database name
            user (str): Database username
            password (str): Database password
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine instance
            connection (sqlalchemy.engine.Connection): SQLAlchemy connection instance
        """
        self.host = host or os.getenv("POSTGRES_HOST")
        self.database = database or os.getenv("POSTGRES_DB")
        self.user = user or os.getenv("POSTGRES_USER")
        self.password = password or os.getenv("POSTGRES_PASSWORD")
        self.engine = None
        self.connection = None

    def __enter__(self):
        """
        Connect to the database when entering the context manager.
        """
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}",
            isolation_level="AUTOCOMMIT",
        )
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Clean up resources when exiting the context manager.
        """
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()

    def run_query(self, query: str, read_only: bool = False, params=None):
        """
        Execute a SQL query and return results for SELECT statements.

        Parameters:
        query (str): SQL query to execute
        params (dict, optional): Parameters for parameterized queries

        Returns:
        list: Query results for SELECT queries
        int: Affected row count for other queries
        """
        try:
            # Execute the query with the given parameters
            result = self.connection.execute(text(query), params or {})

            # If the query is a SELECT statement, return the results
            if read_only:
                return result.fetchall()
            # Otherwise, return the affected row count
            return result.rowcount
        except SQLAlchemyError as e:
            # Log an error if the query fails
            print(f"Error executing query: {e}")
            raise

    def import_df(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "fail", index=False
    ):
        """
        Import a pandas DataFrame into a PostgreSQL table.

        Parameters:
        df (pd.DataFrame): DataFrame to import
        table_name (str): Target table name
        if_exists (str): {'fail', 'replace', 'append'}, default 'fail'
        index (bool): Write DataFrame index as a column

        Raises:
        SQLAlchemyError: If there is an error importing the DataFrame
        """
        try:
            # Use the to_sql method to import the DataFrame
            df.to_sql(
                name=table_name, con=self.connection, if_exists=if_exists, index=index
            )
        except SQLAlchemyError as e:
            # Print the error and raise an exception if the import fails
            print(f"Error importing DataFrame: {e}")
            raise
