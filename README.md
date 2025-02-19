# Python Data Reader Application

This project provides a flexible data reading solution for Python applications, focusing on environment setup, logging configuration, and customizable data source handling.

The Data Reader Application is designed to simplify the process of reading and processing data from various sources. It offers a modular structure that allows for easy integration into existing Python projects. The application's core functionality is encapsulated in the `DataReader` class, which can be initialized with custom parameters to suit different data reading requirements.

Key features of this application include:

- Flexible data source configuration
- Automated environment variable loading
- Configurable logging system
- Docker support for containerized deployment
- Easy installation and setup process

## Repository Structure

- `Dockerfile`: Contains instructions for building the Docker image
- `README.md`: This file, providing project documentation
- `setup.py`: Python setup file for package management and distribution
- `src/`: Source code directory
  - `core/`: Core application logic
    - `data_reader.py`: Main `DataReader` class implementation

## Usage Instructions

### Installation

Prerequisites:

- Python 3.11 or higher
- pip (Python package installer)

To install the application, follow these steps:

1. Clone the repository:

   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```

2. Install the package and its dependencies:

   ```bash
   pip install .
   ```

### Getting Started

To use the `DataReader` class in your Python script:

```python
from src.core.data_reader import DataReader

# Initialize the DataReader
reader = DataReader(data_path="/path/to/your/data", data_source="your_data_source")

# Use the reader object for further operations
# ...
```

### Configuration

The `DataReader` class accepts the following parameters:

- `data_path` (required): Path to the data directory
- `data_source` (optional): Identifier for the data source
- Additional keyword arguments can be passed for custom configuration

### Logging

The application uses Python's built-in logging module. Logs are formatted as follows:

```bash
    YYYY-MM-DD HH:MM:SS - module_name - LOG_LEVEL - Message - line: line_number
```

To adjust the logging level, modify the `logging.basicConfig()` call in `data_reader.py`.

### Environment Variables

The application uses `python-dotenv` to load environment variables. Create a `.env` file in the project root to define custom environment variables.

### Docker Usage

To build and run the application using Docker:

1. Build the Docker image:

   ```bash
   docker build -t data-reader-app .
   ```

2. Run the container:

   ```bash
   docker run -it --rm data-reader-app python
   ```

This will start a Python interactive shell within the container where you can import and use the `DataReader` class.

### Troubleshooting

#### Issue: ImportError when trying to use DataReader

Problem: You may encounter an `ImportError` when trying to import the `DataReader` class.

Solution:

1. Ensure you've installed the package using `pip install .`
2. Check that you're running Python from the correct environment
3. Verify that the `PYTHONPATH` includes the project root directory

If the issue persists, try adding the following at the beginning of your script:

```python
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
```

#### Debugging

To enable debug logging:

1. Modify the logging configuration in `data_reader.py`:

   ```python
   logging.basicConfig(
       level=logging.DEBUG,
       # ... other configurations ...
   )
   ```

2. Re-run your script. You should now see more detailed log output.

Log files are typically output to the console. To save logs to a file, add a `filename` parameter to the `logging.basicConfig()` call.

## Data Flow

The data flow in this application follows these steps:

1. Environment Setup: The application loads environment variables and configures logging.
2. DataReader Initialization: A `DataReader` object is created with specified parameters.
3. Data Source Connection: The `DataReader` connects to the specified data source.
4. Data Reading: Data is read from the source based on the provided configuration.
5. Data Processing: (Not implemented in the current version, but can be added as needed)
6. Output: Processed data is returned or stored as per the implementation.

```bash
[Environment Setup] -> [DataReader Initialization] -> [Data Source Connection]
                                                            |
                                                            v
                        [Output] <- [Data Processing] <- [Data Reading]
```

Note: The actual data processing and output steps would need to be implemented based on specific use case requirements.
