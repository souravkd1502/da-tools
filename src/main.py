from pprint import pprint

from utils.data_reader import DataReader
from utils.data_validation import DataValidation
from utils.metadata_manager import MetadataManager
from src.core.schema_inference import SchemaInference

# Load the dataset
data_reader = DataReader(data_path="data\\tenders_data.csv")
df = data_reader.load_data()

# Initialize the DataValidation class
dv_summary = DataValidation.generate_summary(df)

# Initialize the MetadataManager class
mm = MetadataManager(df, dashboard_name="Tenders_data")
metadata = mm.generate_metadata(summary=dv_summary)

mm.save_metadata()
mm.retrieve_metadata(dashboard_name="Tenders_data")

schema_inference = SchemaInference()
schema_data = schema_inference.infer_schema(metadata)
print("-"*150)
print("Schema Inference:")
print("*"*150)
pprint(schema_data['choices'][0]['message']['content'])
print("*"*150)