How to Run

Install dependencies:

pip install pandas kaggle apache-airflow sqlite3

Configure your Kaggle API:

~/.kaggle/kaggle.json

Place the Python file inside your Airflow DAGs folder:

~/airflow/dags/

Start Airflow:

airflow scheduler
airflow webserver

Trigger the DAG from the Airflow UI.


Extract

Authenticates with Kaggle.

Downloads the dataset

Unzips the weatherHistory.csv file.

Shares the extracted file path with downstream tasks via Airflow XCom.

Transform

Cleans and processes raw data:

Converts timestamps & removes duplicates.

Filters out rows with missing essential values.

Calculates daily averages:

Temperature

Humidity

Wind Speed

Categorizes wind strength (Calm → Violent Storm).

Computes monthly averages and monthly mode precipitation type.

Outputs:

daily_weather.csv

monthly_weather.csv

Validate

Ensures data quality:

Checks for missing values.

Validates temperature, humidity, and wind speed ranges.

Raises errors if inconsistencies are found.

Load

Loads both daily and monthly CSVs into a SQLite database:

daily_weather table

monthly_weather table

Author: - Abhay Prabodh / Abhaywastaken
