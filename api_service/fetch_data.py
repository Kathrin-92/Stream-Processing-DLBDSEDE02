# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import os

api_url = "https://www.umweltbundesamt.de/api/air_data/v3/airquality/json"
headers_data = {"accept": "application/json"}
sensor_data_directory = "api_service/sensor_data"
os.makedirs(sensor_data_directory, exist_ok=True)


# ----------------------------------------------------------------------------------------------------------------------
# API CALL FOR METADATA ON COMPONENTS
# ----------------------------------------------------------------------------------------------------------------------

## get info on components
# this should be done only once and then saved to a file
# url_components = "https://www.umweltbundesamt.de/api/air_data/v3/components/json"
# params = {"lang": "en", "index": "id"}
# headers = {"accept": "application/json"}
# response_components = requests.get(url_components, headers=headers, params=params)
#
# if response_components.status_code == 200:
#     components_data = response_components.json()
#
#     extracted_components_data = {key: value for key, value in components_data.items() if key.isdigit()}
#     df_components = pd.DataFrame.from_dict(extracted_components_data,
#                                 orient="index",
#                                 columns=["ID", "Code", "Symbol", "Unit", "Translated name"])
# else:
#     print(f"Error: {response_components.status_code}, {response_components.text}")


# ----------------------------------------------------------------------------------------------------------------------
# FUNCTIONS FOR API CALL - AIRQUALITY DATA
# ----------------------------------------------------------------------------------------------------------------------

## function to call api
def fetch_data(station_id, date_from, time_from, date_to, time_to):
    params_data = {
        "date_from": date_from,
        "time_from": time_from,
        "date_to": date_to,
        "time_to": time_to,
        "station": station_id
    }
    response = requests.get(api_url, headers=headers_data, params=params_data)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for station {station_id}: {response.status_code}")
        return None

## function to process json data and retrieve only necessary information
def process_data(json_data):
    if not json_data or "data" not in json_data:
        return None

    station_id = list(json_data["data"].keys())[0]
    station_data = json_data["data"][station_id]  # data for the station

    components_data = []

    for datetime_from, entry in station_data.items():
        datetime_to = entry[0]
        components = entry[3:]  # Extract air quality components

        for component in components:
            component_id = component[0]
            value = component[1]

            components_data.append([station_id, datetime_from, datetime_to, component_id, value])

    df = pd.DataFrame(components_data, columns=["station_id", "datetime_from", "datetime_to", "component_id", "value"])
    return df


def save_data(df):
    if df is None or df.empty:
        print("No data to save.")
        return

    for _, row in df.iterrows():
        dt_object = datetime.strptime(row['datetime_from'], "%Y-%m-%d %H:%M:%S")
        timestamp_str = dt_object.strftime("%Y%m%d_%H%M") # convert to nicer format for filename

        filename = f"{sensor_data_directory}/sensor_{row['station_id']}_component_{row['component_id']}_{timestamp_str}.csv"
        row_df = pd.DataFrame([row])
        row_df.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)
        print(f"Saved: {filename}")
        print("Waiting 10 seconds before processing the next component...")
        time.sleep(10)


if __name__ == "__main__":
    station_id_list = [
        "DEHH082" #, "DEHH068", "DEHH008", "DEHH064"
        # , "DEHH015", "DEHH081", "DEHH026", "DEHH070", "DEHH079", "DENI063",
        #"DESH022", "DESH056", "DESH057", "DESH008", "DESH015", "DESH053", "DESH027", "DESH023", "DESH058", "DENW134",
        #"DENW021", "DENW024", "DENW208", "DENW112", "DENW043", "DENW188", "DENW071", "DEBE051", "DEBE010", "DEBE032",
        #"DEBE061", "DEBE069", "DEBE125", "DEBE056", "DEBE127", "DEBE126", "DEBE034", "DEBE065", "DEBE068", "DENW212"
    ]

    start_time = datetime(2025, 2, 28, 23, 0)
    time_interval = timedelta(hours=1)
    num_iterations = 2

    for _ in range(num_iterations):
        headers_data = {"accept": "application/json"}
        date_from = start_time.strftime("%Y-%m-%d")
        time_from = str(start_time.hour)
        end_time = start_time + time_interval
        date_to = end_time.strftime("%Y-%m-%d")
        time_to = str(end_time.hour)

        for station_id in station_id_list:
            print(f"Fetching data for station: {station_id}")
            json_data = fetch_data(station_id, date_from, time_from, date_to, time_to)

            if json_data:
                df = process_data(json_data)
                save_data(df)

        start_time += time_interval
        print(f"Time interval increased to: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
