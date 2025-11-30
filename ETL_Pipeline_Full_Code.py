import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from zipfile import ZipFile
import kaggle
import sqlite3


DATA_DIR = "/home/mehjabin/airflow/weather_project/data"
RAW_ZIP = f"{DATA_DIR}/weather-dataset.zip"
RAW_FILE = f"{DATA_DIR}/weatherHistory.csv"
DAILY_OUT = "/home/mehjabin/airflow/weather_project/output/daily_weather.csv"
MONTHLY_OUT = "/home/mehjabin/airflow/weather_project/output/monthly_weather.csv"
DB_PATH = "/home/mehjabin/airflow/weather_project/weather.db"


# ------------------------- EXTRACT -------------------------
def extract_data(**context):
    kaggle.api.authenticate()

    kaggle.api.dataset_download_files(
        "muthuj7/weather-dataset",
        path=DATA_DIR,
        unzip=False
    )

    with ZipFile(RAW_ZIP, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

    context["ti"].xcom_push(key="raw_file_path", value=RAW_FILE)


# ------------------------- TRANSFORM -------------------------
def transform_data(**context):
    file_path = context["ti"].xcom_pull(key="raw_file_path")
    df = pd.read_csv(file_path)

    # Convert timestamp (handles timezone issues)
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], utc=True)
    df["Formatted Date"] = df["Formatted Date"].dt.tz_convert(None)

    df = df.drop_duplicates()
    df = df.dropna(subset=["Temperature (C)", "Humidity", "Wind Speed (km/h)"])

    df = df.set_index("Formatted Date")

    # Select numeric columns only
    numeric_cols = [
        "Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Visibility (km)",
        "Pressure (millibars)",
    ]

    numeric_df = df[numeric_cols]

    # Daily averages on numeric data only
    daily = numeric_df.resample("D").mean()

    daily["avg_temperature_c"] = daily["Temperature (C)"]
    daily["avg_humidity"] = daily["Humidity"]
    daily["avg_wind_speed_kmh"] = daily["Wind Speed (km/h)"]
    daily.reset_index(inplace=True)

    # Wind strength categories
    def categorize_wind(speed):
        if speed <= 1.5: return "Calm"
        if speed <= 3.3: return "Light Air"
        if speed <= 5.4: return "Light Breeze"
        if speed <= 7.9: return "Gentle Breeze"
        if speed <= 10.7: return "Moderate Breeze"
        if speed <= 13.8: return "Fresh Breeze"
        if speed <= 17.1: return "Strong Breeze"
        if speed <= 20.7: return "Near Gale"
        if speed <= 24.4: return "Gale"
        if speed <= 28.4: return "Strong Gale"
        if speed <= 32.6: return "Storm"
        return "Violent Storm"

    daily["wind_strength"] = daily["avg_wind_speed_kmh"].apply(categorize_wind)
    daily.to_csv(DAILY_OUT, index=False)

    # Monthly mode precipitation
    monthly_mode = df["Precip Type"].resample("M").agg(
        lambda x: x.mode()[0] if not x.mode().empty else None
    )

    # Monthly averages based ONLY on numeric columns
    monthly = numeric_df.resample("M").mean()
    monthly["Mode Precip Type"] = monthly_mode
    monthly.reset_index(inplace=True)

    monthly.to_csv(MONTHLY_OUT, index=False)

    context["ti"].xcom_push(key="daily_csv", value=DAILY_OUT)
    context["ti"].xcom_push(key="monthly_csv", value=MONTHLY_OUT)



# ------------------------- VALIDATE -------------------------
def validate_data(**context):
    daily_file = context["ti"].xcom_pull(key="daily_csv")
    monthly_file = context["ti"].xcom_pull(key="monthly_csv")

    d = pd.read_csv(daily_file)

    if d.isna().sum().sum() > 0:
        raise ValueError("Missing values detected in daily data.")

    if ((d["avg_temperature_c"] < -50) | (d["avg_temperature_c"] > 50)).any():
        raise ValueError("Temperature out of range (-50 to 50 C).")

    if ((d["avg_humidity"] < 0) | (d["avg_humidity"] > 1)).any():
        raise ValueError("Humidity out of range (0 to 1).")

    if (d["avg_wind_speed_kmh"] < 0).any():
        raise ValueError("Negative wind speed detected.")

    return "VALIDATED"


# ------------------------- LOAD -------------------------
def load_data(**context):
    daily_file = context["ti"].xcom_pull(key="daily_csv")
    monthly_file = context["ti"].xcom_pull(key="monthly_csv")

    daily = pd.read_csv(daily_file)
    monthly = pd.read_csv(monthly_file)

    conn = sqlite3.connect(DB_PATH)

    daily.to_sql("daily_weather", conn, if_exists="replace", index=False)
    monthly.to_sql("monthly_weather", conn, if_exists="replace", index=False)

    conn.close()


# ------------------------- DAG -------------------------
default_args = {
    "owner": "mehjabin",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="weather_etl_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    extract >> transform >> validate >> load
