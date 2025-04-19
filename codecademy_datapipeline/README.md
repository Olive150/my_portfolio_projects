# Subscriber Cancellations Data Pipeline

This project includes a data pipeline for processing and cleaning subscriber cancellations data. The pipeline reads data from two SQLite databases (`cademycode.db` and `cademycode_updated.db`), processes the data, and exports cleaned data to both a SQLite database and a CSV file.

To run the project, from the root directory use: 
- chmod +x deploy.sh
- ./deploy.sh

To run the test , from the dev directory:
- python3 test_pipeline.py

## Project Structure

- `pipeline_script.py`: Main Python script that contains the data processing pipeline logic.
- `pipeline.ipynb`: Jupyter notebook with the same functionality as the Python script but displays results interactively.
- `deploy.sh`: Bash script to automate running the Python pipeline and moving the output files.
- `script_log.log`: Log file that tracks the execution of the script and any issues that arise.
- `dev/`: Directory containing development files, including databases and scripts.
- `production/`: Directory for storing the final output files after the pipeline runs.
- `test_pipeline.py`: Python script containing unit tests for `pipeline_script.py`.

## Requirements

Make sure the following Python packages are installed:

- pandas
- sqlite3
- json

