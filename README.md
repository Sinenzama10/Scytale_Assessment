# Scytale_Assessment - Pull Request Aggregator

## Description

This project is designed to aggregate data from pull requests across all repositories in the `Scytale-exercise` GitHub organization. It extracts data using the GitHub API, transforms it with PySpark to calculate various metrics, and then loads the transformed data into a Parquet file. This solution helps in analyzing pull request activities within the organization.

## Prerequisites

Before running this project, ensure the following are installed on your system:

- **Java**: Required by PySpark for data processing.
- **Python**: Version 3.6 or newer, necessary for running the script and PySpark operations.

### Checking Installations

- **Java**:
  - To verify Java installation, open a terminal and run:
    ```bash
    java -version
    ```
  - If Java is installed, the version number will be displayed.

- **Python**:
  - Check Python installation by running:
    ```bash
    python --version
    ```
  - The installed Python version should be displayed.

### Installing Java and Python

- **Java**:
  - **macOS**: Install via [Homebrew](https://brew.sh/) with `brew install openjdk`.
  - Visit [Oracle's website](https://www.java.com/en/download/) for detailed installation instructions on other platforms.

- **Python**:
  - Download and install Python from the [official Python website](https://www.python.org/downloads/).

## Environment Setup

### Generating a GitHub Personal Access Token

1. Navigate to your GitHub settings.
2. Go to Developer settings > Personal access tokens.
3. Click "Generate new token" and select the necessary permissions (e.g., `repo`).
4. Copy the generated token.

## Setup and Running

1. **Clone the Repository**:
   - Use `git clone https://github.com/Sinenzama10/Scytale_Assessment.git` to clone this project to your local machine.

2. **Install Dependencies**:
   - Navigate to the project directory and run:
     ```bash
     pip install -r requirements.txt
     ```

3. **Environment Variables**:
   - Set the `GITHUB_TOKEN` environment variable with your GitHub personal access token:
     - **macOS/Linux**:
       ```bash
       export GITHUB_TOKEN='your_token_here'
       ```
     - Add this line to your `~/.bash_profile`, `~/.bashrc`, or `~/.zshrc` and run `source ~/.bashrc` (or equivalent) to apply.

4. **Running the Project**:
   - Execute the main script with:
     ```bash
     python src/main.py
     ```

## Project Structure

- `src/`: Contains the source code.
  - `main.py`: The main script to run for executing the ETL process.
  - `extract.py`: Module for fetching data from GitHub.
  - `spark_process.py`: Module for processing data with PySpark.
- `data/`: Directory for storing fetched JSON data.
- `output/`: Directory where the transformed Parquet files are saved.
- `requirements.txt`: Lists the Python package dependencies.

## Additional Notes
You might find included in the project directory a .ipynb called Spark_Process_Book.ipynb you can ignore this Notebook and don't have to run it. I was using this to build the spark processing queries and also to validate the output of the parquet file, then I transferred it all over to my spark_process.py Notebook. This was simply due to the fact that I am used to using notebooks to visualize outputs because my current company uses Databricks at the moment which is very Notebok orientated.
