# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
import pandas as pd
import time
from datetime import timedelta, date
import os


# ----------------------------------------------------------------------------------------------------------------------
# FUNCTIONS FOR STREAM SIMULATION
# ----------------------------------------------------------------------------------------------------------------------

def load_batch_data():
    batch_directory = "api_service/sensor_data/batch_data"
    date_obj = date.today() - timedelta(days=1)
    date_str = date_obj.strftime("%Y%m%d")
    file_name = f"batch_data_{date_str}.csv"
    batch_file_path = os.path.join(batch_directory, file_name)

    # load latest batch file
    df = pd.read_csv(batch_file_path)
    df["datetime_from"] = pd.to_datetime(df["datetime_from"])
    df["datetime_to"] = pd.to_datetime(df["datetime_to"])
    df["timestamp_str"] = df["datetime_from"].dt.strftime("%Y%m%d_%H%M") # convert to nicer format for filename
    df.sort_values(by=["datetime_from"], inplace=True)

    # convert to unix for json
    df["datetime_from"] = df["datetime_from"].astype(int) // 10 ** 9
    df["datetime_to"] = df["datetime_to"].astype(int) // 10 ** 9
    return df


def simulate_stream(df):
    if df is None or df.empty:
        print("No data to process.")
        return
    for _, row in df.iterrows():
        sensor_data_directory = "api_service/sensor_data/stream_data"
        os.makedirs(sensor_data_directory, exist_ok=True)
        filename = f"{sensor_data_directory}/sensor_{row['station_id']}_component_{row['component_id']}_{row['timestamp_str']}.json"
        row_df = pd.DataFrame([row])
        row_df.to_json(filename, mode='w', orient='records')
        print(f"Saved: {filename}")
        print("Waiting 10 seconds before processing the next row...") # to do: make counter more readable
        time.sleep(10)