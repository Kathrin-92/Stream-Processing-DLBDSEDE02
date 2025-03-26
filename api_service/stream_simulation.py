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
    """
        Loads the latest (always yesterday) .csv file of batch data from directory.
        Performs some date transformations for readability.
        Returns: df of batch file
    """
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
    """
        Simulates a stream of data by reading the df of batch data row by row and saving each row as its own file.
        Waits 10 seconds before processing the next row.
    """
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


def delete_old_files():
    """
        Function for deleting old files from directory after 6 Minutes (360 seconds).
    """
    sensor_data_directory = "api_service/sensor_data/stream_data"
    os.makedirs(sensor_data_directory, exist_ok=True)
    current_time = time.time()

    for dirpath, dirnames, filenames in os.walk(sensor_data_directory, topdown=True):
        for f in filenames:
            file_location = os.path.join(dirpath, f)
            file_time = os.stat(file_location).st_mtime
            if file_time < current_time - 360:
                print(f" Deleting file : {file_location}")
                os.remove(file_location)


def periodic_delete():
    """
        Starts the delete_old_files job every 60 seconds to periodically delete old data files.
    """
    while True:
        delete_old_files()
        time.sleep(60)