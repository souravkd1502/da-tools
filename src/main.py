from utils.data_reader import DataReader
from utils.data_validation import DataValidation
from utils.metadata_manager import MetadataManager

from pprint import pprint

# Load the dataset
data_reader = DataReader(data_path="data\\tenders_data.csv")
df = data_reader.load_data()

# Initialize the DataValidation class
dv_summary = DataValidation.generate_summary(df)

# Initialize the MetadataManager class
mm = MetadataManager(df, dashboard_name="Tenders_data")
mm.generate_metadata()
mm.save_metadata()

retrieved_meta = mm.retrieve_metadata(dashboard_name="Tenders_data")
print("-" * 150)
