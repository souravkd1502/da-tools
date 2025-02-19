"""

"""

# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
import boto3
import logging
import warnings
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from botocore.exceptions import BotoCoreError
from azure.storage.blob import BlobServiceClient

from typing import List, Optional, Union

# Set up logging
_logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - line: %(lineno)d",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Environment variables
load_dotenv(override=True)


class DataLoadingError(Exception):
    """Custom exception for data loading failures"""

    pass


class DataReader:
    """
    A robust data loader supporting multiple file formats with enhanced error handling
    and performance optimizations.

    Features:
    - Supports CSV, Parquet, Excel (multi-sheet), and JSON formats
    - Automatic file type detection
    - Comprehensive error handling and validation

    Args:
        data_path (str): Path to the data file
        data_source (str, optional): Source system identifier (e.g., 's3', 'database')
        **kwargs: Additional arguments passed to pandas read functions

    Raises:
        DataLoadingError: On any data loading failure
        ValueError: For invalid file paths or unsupported formats

    TODO: Add support for blob reader (Azure and S3) strictly for CSV, Parquet, Excel, and JSON formats
    TODO: Add support for reading data from databases (MS SQL, MySQL, PostgreSQL, etc.)

    Examples:
    # Example Usage of `DataReader`

    ## 1. Local CSV File
    ```python
    reader = DataReader("data.csv")
    df = reader.load_data()
    print(df.head())
    ```

    ## 2. Local Parquet File
    ```python
    reader = DataReader("data.parquet")
    df = reader.load_data()
    print(df.head())
    ```

    ## 3. Local Excel File
    ```python
    reader = DataReader("data.xlsx", sheets=["Sheet1", "Sheet2"])
    sheets = reader.load_data()

    for idx, sheet in enumerate(sheets):
        print(f"Sheet {idx + 1}:\n", sheet.head())
    ```

    ## 4. Local JSON File
    ```python
    reader = DataReader("data.json")
    df = reader.load_data()
    print(df.head())
    ```

    ## 5. Azure Blob Storage (CSV)
    ```python
    azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"

    reader = DataReader(
        data_path="azure://my-container/data.csv",
        data_source="azure",
        connection_string=azure_connection_string
    )

    df = reader.load_data()
    print(df.head())
    ```

    ## 6. S3 Bucket (Parquet)
    ```python
    reader = DataReader(
        data_path="s3://my-bucket/data.parquet",
        data_source="s3",
        aws_access_key="your_access_key",
        aws_secret_key="your_secret_key",
        aws_region="your_region"
    )

    df = reader.load_data()
    print(df.head())
    ```

    ## 7. Postgres Database
    ```python
    reader = DataReader(
            data_path="<>",
            data_source="database",
            connection_string = 'postgresql+psycopg2://user:password@hostname/database_name',
            query = 'SELECT * FROM public."table_name"',
        )

        df = reader.load_data()
        print(df.head())
    ```
    """

    SUPPORTED_FORMATS = {"csv", "parquet", "xlsx", "json"}
    EXCEL_ENGINES = {"openpyxl"}
    PARQUET_ENGINES = {"auto", "pyarrow", "fastparquet"}

    def __init__(
        self, data_path: str, data_source: Optional[str] = None, **kwargs
    ) -> None:
        """
        Initialize the DataReader with validation and path resolution.

        The DataReader is initialized with a data path and optional data source.
        The data path is validated to ensure it exists and is accessible, and
        the file type is automatically detected using the file extension.

        Args:
            data_path: Path to the data file
            data_source: Source system identifier (e.g., 's3', 'database')
            kwargs: Additional arguments passed to pandas read functions

        Example:
            >>> reader = DataReader('data.csv')
            >>> df = reader.load_data()
        """
        self.data_path = Path(data_path)
        self.data_source = data_source
        self.kwargs = kwargs
        if data_source != "database":
            self._file_type = self._get_file_type()

            _logger.debug(
                f"Initialized DataReader for {self.data_path} (Type: {self._file_type})"
            )

        if data_source not in ["s3", "azure", "database"]:
            self._validate_path(data_path)

    def _validate_path(self, path: str) -> None:
        """Validate data path exists and is accessible"""
        if not Path(path).exists():
            _logger.error(f"Path not found: {path}")
            raise FileNotFoundError(f"Data file not found: {path}")
        if not Path(path).is_file():
            _logger.error(f"Path is not a file: {path}")
            raise ValueError(f"Not a file: {path}")

    def _get_file_type(self) -> str:
        """
        Detect file type using path suffix with validation.

        Returns:
            str: Lowercase file extension without dot

        Raises:
            ValueError: For unsupported file formats
        """
        suffix = self.data_path.suffix.lower()[1:]  # Remove leading dot
        if suffix not in self.SUPPORTED_FORMATS:
            _logger.error(f"Unsupported file format: {suffix}")
            raise ValueError(
                f"Unsupported format: {suffix}. Supported: {self.SUPPORTED_FORMATS}"
            )
        return suffix

    def load_data(self) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Load data from the specified source path.

        Depending on the source system, the data is loaded from a file, blob storage,
        or a database. The data is then passed to the appropriate load function.

        Args:
            None

        Returns:
            Union[pd.DataFrame, List[pd.DataFrame]]: Loaded data

        Raises:
            DataLoadingError: On any data loading failure
        """
        _logger.info(f"Loading data from {self.data_source or self.data_path}")
        try:
            # Load from S3
            if self.data_source == "s3":
                return self._load_from_s3()
            # Load from Azure Blob Storage
            elif self.data_source == "azure":
                return self._load_from_azure()
            # Load from a database
            elif self.data_source == "database":
                return self._load_from_database()
            # Load from a file
            else:
                return self._load_from_file()
        except Exception as e:
            _logger.exception(
                f"Failed to load {self.data_source or self.data_path}: {str(e)}"
            )
            raise DataLoadingError(f"Data loading failed: {str(e)}") from e

    def _load_from_s3(self) -> pd.DataFrame:
        """Load data from an S3 bucket

        This method retrieves an object from S3 and loads it into a DataFrame.

        Returns:
            pd.DataFrame: The loaded data

        Raises:
            DataLoadingError: If there is an error accessing S3
        """
        try:
            # Initialize S3 client
            s3 = boto3.client("s3")

            # Parse bucket and key from the data path
            bucket, key = self.data_path.split("/", 1)

            # Retrieve the object from S3
            obj = s3.get_object(Bucket=bucket, Key=key)

            # Load the object content into a DataFrame
            return self._load_from_file_object(obj["Body"].read())
        except BotoCoreError as e:
            # Raise a custom error if there's a problem with S3 access
            raise DataLoadingError(f"S3 error: {e}") from e

    def _load_from_azure(self) -> pd.DataFrame:
        """
        Load data from Azure Blob Storage.

        This method connects to Azure Blob Storage, downloads the specified blob,
        and loads its content into a DataFrame.

        Returns:
            pd.DataFrame: The loaded data.

        Raises:
            DataLoadingError: If there is an error accessing Azure Blob Storage.
        """
        try:
            # Initialize BlobServiceClient using connection string
            blob_service = BlobServiceClient.from_connection_string(
                self.kwargs["connection_string"]
            )

            # Get the BlobClient for the specified container and blob (data_path)
            blob_client = blob_service.get_blob_client(
                container=self.kwargs["container"], blob=self.data_path
            )

            # Download the blob as a stream
            stream = blob_client.download_blob()

            # Load the stream content into a DataFrame
            return self._load_from_file_object(stream.readall())

        except Exception as e:
            # Raise a custom error if there's a problem with Azure Blob access
            raise DataLoadingError(f"Azure Blob error: {e}") from e

    def _load_from_file_object(self, file_data: bytes) -> pd.DataFrame:
        """
        Load data from file object into a DataFrame.

        This method takes a file object and loads its content into a DataFrame.
        It supports CSV, Parquet, Excel, and JSON files.

        Args:
            file_data (bytes): The file data to be loaded.

        Returns:
            pd.DataFrame: The loaded data.

        Raises:
            DataLoadingError: If there is an error loading the file or if the file
                format is not supported.
        """
        if self._file_type == "csv":
            # Load CSV file using pd.read_csv
            return pd.read_csv(pd.io.common.BytesIO(file_data))
        elif self._file_type == "parquet":
            # Load Parquet file using pd.read_parquet
            return pd.read_parquet(pd.io.common.BytesIO(file_data))
        elif self._file_type == "xlsx":
            # Load Excel file using pd.read_excel
            return pd.read_excel(pd.io.common.BytesIO(file_data))
        elif self._file_type == "json":
            # Load JSON file using pd.read_json
            return pd.read_json(pd.io.common.BytesIO(file_data))
        raise DataLoadingError(
            "Unsupported file format for blob storage. Supported formats: CSV, "
            "Parquet, Excel, and JSON."
        )

    def _load_from_database(self) -> pd.DataFrame:
        """
        Load data from a database using an SQL query.

        This method connects to a database using the provided connection string,
        executes the SQL query, and loads the result into a DataFrame.

        Args:
            None

        Returns:
            pd.DataFrame: The loaded data.

        Raises:
            DataLoadingError: If there is an error accessing the database.
        """
        try:
            # Initialize SQLAlchemy engine
            engine = create_engine(self.kwargs["connection_string"])

            # Execute the SQL query
            # Note: The query should be a SELECT statement
            df = pd.read_sql(self.kwargs["query"], con=engine)

            # Validate the DataFrame
            self._validate_df(df)

            return df
        except Exception as e:
            # Raise a custom error if there's a problem with database access
            raise DataLoadingError(f"Database error: {e}") from e

    def _load_from_file(self) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """
        Load data with format-specific handling and performance optimizations.

        Returns:
            Union[pd.DataFrame, List[pd.DataFrame]]: Loaded data

        Raises:
            DataLoadingError: On any data loading failure

        Example:
            >>> reader = DataReader('data.xlsx')
            >>> sheets = reader.load_data(sheets=['Sheet1'])
        """

        try:
            if self._file_type == "csv":
                return self._load_csv()
            elif self._file_type == "parquet":
                return self._load_parquet()
            elif self._file_type == "xlsx":
                return self._load_excel()
            elif self._file_type == "json":
                return self._load_json()
            else:
                # This should never be reached due to _get_file_type validation
                raise DataLoadingError(f"Unsupported file type: {self._file_type}")
        except Exception as e:
            _logger.exception(f"Failed to load {self.data_path}: {str(e)}")
            raise DataLoadingError(f"Data loading failed: {str(e)}") from e

    def _load_csv(self) -> pd.DataFrame:
        """
        Load CSV file with performance optimizations.

        Returns:
            pd.DataFrame: Loaded data

        Raises:
            DataLoadingError: On any data loading failure
        """
        try:
            # Read the CSV file into a DataFrame with performance optimizations
            df = pd.read_csv(self.data_path)

            # Validate the loaded DataFrame
            self._validate_df(df)

            return df
        except pd.errors.ParserError as pe:
            # Raise a custom error if there's a problem with CSV parsing
            _logger.error("CSV parsing error: %s", pe)
            raise DataLoadingError(f"CSV parsing error: {pe}") from pe

    def _load_parquet(self) -> pd.DataFrame:
        """Load Parquet file with engine validation

        The engine parameter controls the library used to read the Parquet file.
        The default is "auto", which uses the most efficient engine available.
        The supported engines are:

        - auto: The most efficient engine available (default)
        - pyarrow: The PyArrow library
        - fastparquet: The FastParquet library

        Returns:
            pd.DataFrame: Loaded data

        Raises:
            DataLoadingError: On any data loading failure
        """
        engine = self.kwargs.get("engine", "auto")
        if engine not in self.PARQUET_ENGINES:
            raise DataLoadingError(f"Invalid Parquet engine: {engine}")

        return pd.read_parquet(self.data_path, engine=engine)

    def _load_excel(self) -> List[pd.DataFrame]:
        """
        Load Excel file with multi-sheet handling.

        Args:
            None

        Returns:
            List[pd.DataFrame]: Loaded data

        Raises:
            DataLoadingError: On any data loading failure
        """
        sheets = self.kwargs.get("sheets", None)

        try:
            # Read the Excel file into a list of DataFrames
            with pd.ExcelFile(self.data_path) as excel:
                # If sheets is specified, only load the specified sheets
                if sheets:
                    return [excel.parse(sheet) for sheet in sheets]
                # Otherwise, load all sheets
                return [excel.parse(sheet_name) for sheet_name in excel.sheet_names]
        except (ValueError, ImportError) as e:
            # Raise a custom error if there's a problem with Excel loading
            _logger.error("Excel loading error: %s", e)
            raise DataLoadingError(f"Excel loading failed: {e}") from e

    def _load_json(self) -> pd.DataFrame:
        """
        Load JSON data with format detection

        The ``pd.read_json`` function is used to load the JSON data. It
        automatically detects the JSON format (e.g. records, index, etc.) and
        loads the data accordingly.

        Returns:
            pd.DataFrame: Loaded data
        """
        return pd.read_json(self.data_path)

    def _validate_df(self, df: pd.DataFrame) -> None:
        """
        Perform post-load validation of the loaded DataFrame.

        This includes checking for empty DataFrames and missing values.

        Args:
            df (pd.DataFrame): The loaded DataFrame
        """
        if df.empty:
            warnings.warn(f"Empty DataFrame loaded from {self.data_path}")
            _logger.warning("Loaded empty DataFrame")

        missing_values = df.isna().sum().sum()
        if missing_values > 0:
            _logger.info("Dataset contains %d missing values", missing_values)

    @property
    def detected_format(self) -> str:
        """
        Property to get the detected file format.

        Returns:
            str: The file type detected during initialization.
        """
        # Return the detected file type stored in the _file_type attribute
        return self._file_type
