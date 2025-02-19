"""


"""
# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
import logging
from dotenv import load_dotenv


# Set up logging
_logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - line: %(lineno)d",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Load Environment variables
load_dotenv(override=True)

class Planner:
    """
    The Planner class is responsible for planning the execution of data processing tasks.
    It takes a DataReader object and a DataWriter object as input and provides methods
    to execute the data processing pipeline.
    """

    def __init__(self, reader, writer):
        """
        Initialize the Planner object.

        Args:
            reader (DataReader): The DataReader object to read data from.
            writer (DataWriter): The DataWriter object to write data to.
        """
        self.reader = reader
        self.writer = writer

    def execute(self):
        """
        Execute the data processing pipeline.

        This method reads data from the DataReader object, performs data processing
        tasks, and writes the processed data to the DataWriter object.
        """
        # Read data from the DataReader object
        data = self.reader.load_data()

        # Perform data processing tasks
        # ...

        # Write processed data to the DataWriter object
        self.writer.write_data(data)    