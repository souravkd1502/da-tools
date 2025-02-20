"""

"""

# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
import os
import logging
from dotenv import load_dotenv

from src.core.model import OpenAIChatHandler

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


class SchemaInference:
    def __init__(self, model_name: str = "gpt-4o") -> None:
        """
        Initialize the SchemaInference class.

        Args:
            model_name (str): The name of the model to use for schema inference. Defaults to "gpt-4o".
        """
        self.model_name = model_name
        self.model = OpenAIChatHandler(
            openai_key=os.getenv("OPENAI_API_KEY"), model=model_name
        )

    def infer_schema(self, data: str) -> Dict[str, Any]:
        """
        Infer the schema of the given data using the specified prompt.

        Args:
            data (str): The data to infer the schema for.
            prompt (str): The prompt to use for schema inference.

        Returns:
            str: The inferred schema.
        """
        SYSTEM_PROMPT = """
        **Objective**: Generate a detailed schema inference for the provided data sample and data summary. 
        The schema should accurately describe the structure, data types, constraints, relationships, and any implicit patterns or anomalies.  

        ### Instructions:  
        1. **Analyze the Inputs**:  
        - Review the **data sample** (e.g., raw data rows, CSV/JSON snippets) to infer field names, data types, formats, and potential constraints.  
        - Cross-reference the **data summary** (e.g., statistical overview, missing value counts, unique values, distributions) to validate assumptions and identify hidden patterns.  

        2. **Schema Components**:  
        - **Fields**: List all detected fields with their inferred names.  
        - **Data Types**: Specify types (e.g., `integer`, `string`, `date`, `boolean`) and formats (e.g., `YYYY-MM-DD`, `ISO 8601`).  
        - **Constraints**: Note nullable fields, uniqueness, value ranges, regex patterns, or foreign/key relationships.  
        - **Relationships**: Highlight correlations, dependencies, or hierarchical structures (e.g., "user_id" links to a "users" table).  

        3. **Normalization Recommendations**:  
        - Suggest data normalization steps (e.g., splitting composite fields, standardizing categorical values).  

        4. **Potential Issues**:  
        - Flag inconsistencies between the sample and summary (e.g., mismatched data types, outliers, missing values).  

        5. **Final Summary**:  
        - Provide a concise technical summary of the schema, including primary keys, required fields, and critical constraints.  

        ### Output Format:  
        # Schema Inference  

        ## Fields  
        - **`[field_name]`**: [data_type] ([format, if applicable])  
        - Constraints: [nullable? | unique? | range: X-Y | regex: ...]  
        - Notes: [e.g., "5% missing values per data summary"]  

        ## Relationships  
        - `[field_A]` → `[field_B]` ([relationship type, e.g., one-to-many])  

        ## Normalization Recommendations  
        - [Actionable step, e.g., "Split `address` into `street`, `city`, `zipcode`"]  

        ## Anomalies/Issues  
        - [e.g., "Date format inconsistency in sample vs. ISO standard in summary"]  

        ## Final Summary  
        [Concise schema overview in 1–2 paragraphs.]  
        ```  

        ### Example Inputs for Context:  
        - **Data Sample**:  
        ```csv  
        id,transaction_date,amount,user_id  
        1,2023-01-15,150.50,user_001  
        2,2023-01-16,200.00,user_002  
        ```  
        - **Data Summary**:  
        - `transaction_date`: 100% non-null, format `YYYY-MM-DD`.  
        - `amount`: Range $10–$500, mean $175.  

        ### Expected Output Quality:  
        - Precise, technically sound, and aligned with both sample and summary.  
        - Explicitly state assumptions if data is ambiguous.  

        ---  
        **Begin your analysis using the provided data sample and summary.**
        """

        # Construct the user prompt
        prompt = f"""
        You are a data scientist who is tasked with inferring the schema of a given data. 
        The data is provided in the following format:
        <data>
        {data}
        </data>
        Please infer the schema of the data.
        """
        # Generate the schema using the model
        _logger.info("Generating schema for the data...")
        response = self.model.chat_completion(
            prompt=prompt, system_prompt=SYSTEM_PROMPT, max_tokens=2048
        )
        return response


if __name__ == "__main__":
    # Example usage
    import pandas as pd

    df = pd.read_csv("data\\tenders_data.csv")
    data = df.head()
    schema_inference = SchemaInference()
    schema = schema_inference.infer_schema(data)
    print(schema)
