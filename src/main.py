import json
from pprint import pprint

from utils.data_reader import DataReader
from utils.data_validation import DataValidation
from utils.metadata_manager import MetadataManager
from src.core.schema_inference import SchemaInference
from src.core.planner import Planner
from src.utils.dashboard import DashDashboard

# Load the dataset
data_reader = DataReader(data_path="data\\country_wise_latest.csv")
df = data_reader.load_data()

# Initialize the DataValidation class
dv_summary = DataValidation.generate_summary(df)

# Initialize the MetadataManager class
mm = MetadataManager(df, dashboard_name="covid-19")
metadata = mm.retrieve_metadata(dashboard_name="covid-19")
if metadata is None:
    metadata = mm.generate_metadata(summary=dv_summary)
    mm.save_metadata()

schema_inference = SchemaInference()
schema_data = schema_inference.infer_schema(metadata)
planner = Planner()
execution_plan = planner.generate_plan(
    schema_inference=schema_data["choices"][0]["message"]["content"], metadata=metadata
)
plan = json.loads(execution_plan['choices'][0]['message']['content'])
print("*"* 150)
pprint(plan)
print("*"* 150)

dashboard = DashDashboard(plan, df)
dashboard.run()
