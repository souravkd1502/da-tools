"""


"""

# Adding directories to system path to allow importing custom modules
import sys

sys.path.append("./")
sys.path.append("../")

# Importing necessary libraries and modules
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


class Planner:
    """
    A class to handle planning and execution of tasks.
    """

    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "dashboard",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "The title of the dashboard.",
                    },
                    "data_source": {
                        "type": "string",
                        "description": "The source of the data used in the dashboard.",
                    },
                    "charts": {
                        "type": "array",
                        "description": "A collection of charts displayed on the dashboard.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "description": "The type of the chart (e.g., bar, line).",
                                },
                                "x_axis": {
                                    "type": "string",
                                    "description": "The variable represented on the x-axis.",
                                },
                                "y_axis": {
                                    "type": "string",
                                    "description": "The variable represented on the y-axis.",
                                },
                            },
                            "required": ["type", "x_axis", "y_axis"],
                            "additionalProperties": False,
                        },
                    },
                    "filters": {
                        "type": "array",
                        "description": "A collection of filters available for the dashboard.",
                        "items": {"type": "string", "description": "A filter option."},
                    },
                    "metrics": {
                        "type": "array",
                        "description": "A collection of metrics displayed on the dashboard.",
                        "items": {"type": "string", "description": "A metric option."},
                    },
                },
                "required": ["title", "data_source", "charts", "filters", "metrics"],
                "additionalProperties": False,
            },
        },
    }

    def __init__(self, openai_key: str) -> None:
        """
        Initializes the Planner class with the OpenAI API key.

        Args:
            openai_key (str): The OpenAI API key.
        """
        self.openai_key = openai_key
        self.openai = OpenAIChatHandler(self.openai_key)
        _logger.info("Planner initialized...")

    def generate_plan(self, schema_inference: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates a plan for the given task using schema inference and metadata.
        The plan is a JSON object that contains information about the task, 
        such as the title, data source, charts, filters, and metrics
        
        This function creates a JSON object that outlines the task's details,
        such as the title, data source, charts, filters, and metrics to be used.

        Args:
            schema_inference (str): The inferred schema information.
            metadata (Dict[str, Any]): The metadata containing additional information.

        Returns:
            Dict[str, Any]: The generated plan as a JSON object.
        """
        # Log the start of the planning process
        _logger.info("Planning and executing task.")
        
        # Placeholder for the plan generation logic
        # You would typically process the schema_inference and metadata here
        # to construct the plan structure.
        
        SYSTEM_PROMPT = f"""
        You are provided with a dataset that includes various structured data fields. 
        Your task is to create a detailed and actionable plan based on this data. 
        Follow the steps below:

        Schema Inference:
        {schema_inference}

        Analyze the dataset to infer its underlying schema. Identify key fields, data types, and relationships among the fields.
        Note any missing, anomalous, or unexpected data points that might influence your plan.
        Data Summary:
        {metadata}

        Summarize the main trends, patterns, and statistics derived from the dataset.
        Highlight critical insights that could drive decision-making.
        Chain-of-Thought (CoT) Reasoning:

        Break down your reasoning into clear, step-by-step thoughts.
        Document your thought process as you analyze the schema and data summary, including any assumptions and intermediate conclusions.
        ReACT Style – Reflect, Act, and Check:

        Reflect: Consider the insights from the schema inference and data summary. Ask yourself: What are the most pressing issues or opportunities revealed by the data?
        Act: Based on your reflection, propose specific, actionable steps and strategies that address these issues or opportunities.
        Check: Evaluate your proposed plan against the objectives and constraints outlined by the dataset’s context. 
                Validate that your plan is feasible and aligned with the data insights.
        Final Plan:

        Synthesize your chain-of-thought and ReACT reasoning to produce a comprehensive plan.
        Ensure your final plan includes clear objectives, actionable items, timelines (if applicable), and any contingencies or recommendations.
        Output both your step-by-step reasoning (chain-of-thought) and the final detailed plan. Your response should clearly 
        document how the insights from the data informed each element of the plan.
        """
        
        _logger.info("Generating plan...")
        # Call the OpenAI API to generate the plan
        response = self.openai.chat_completion(
            prompt=schema_inference,
            system_prompt=SYSTEM_PROMPT,
            max_tokens=4096,
            response_format=self.response_format,
        )
        return response

