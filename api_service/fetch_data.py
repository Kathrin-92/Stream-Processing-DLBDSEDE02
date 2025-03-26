# ----------------------------------------------------------------------------------------------------------------------
# IMPORTS
# ----------------------------------------------------------------------------------------------------------------------
import requests
import pandas as pd
from datetime import timedelta, date
import os


# ----------------------------------------------------------------------------------------------------------------------
# FUNCTION FOR METADATA
# ----------------------------------------------------------------------------------------------------------------------

def fetch_components_metadata(language_parameter, index_parameter, api_url_components, header):
    """
    Fetches components metadata from the Umweltbundesamt API and saves it as a CSV file.
    The function checks whether the metadata file already exists before making an API request.
    If the file exists, the function skips the API call. Otherwise, it fetches the metadata.
    The function does not return a value but saves the metadata to a CSV file.
    """

    save_directory = "api_service/sensor_data/metadata"
    file_name = "components_metadata.csv"
    file_path = os.path.join(save_directory, file_name)
    os.makedirs(save_directory, exist_ok=True)

    if os.path.exists(file_path):
        print("Metadata file already exists. No API call needed.")
        return

    else:
        # set parameters and make api call
        parameters = {
            "lang": language_parameter,
            "index": index_parameter
        }
        response_components = requests.get(api_url_components, headers=header, params=parameters)

        if response_components.status_code == 200:
            components_metadata = response_components.json()

            extracted_metadata = {key: value for key, value in components_metadata.items() if key.isdigit()}
            extracted_metadata = pd.DataFrame.from_dict(extracted_metadata,
                                    orient="index",
                                    columns=["id", "code", "symbol", "unit", "name"])
            extracted_metadata.to_csv(file_path, index=False)
            print("Metadata successfully saved.")
        else:
            print(f"Error fetching metadata: {response_components.status_code}, {response_components.text}")


# ----------------------------------------------------------------------------------------------------------------------
# FUNCTIONS FOR BATCH PROCESS TO GET SENSOR DATA
# ----------------------------------------------------------------------------------------------------------------------

def fetch_data(station_id, date_from, time_from, date_to, time_to, headers_data, api_url):
    """
    Fetches sensor data from the API for a given station and timeframe.
    This function sends a request to the API to retrieve sensor data for a specified
    station and time range. If the request is successful, the function returns the
    JSON response. Otherwise, it returns None.
    """

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


def process_batch_data(json_data):
    """
    Processes sensor JSON data and retrieves only necessary information.
    This function extracts relevant sensor data from the JSON response, formats it into a
    DataFrame, and appends it to a CSV file. If the file does not exist, it creates a new one.
    """

    # process data from json
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
    df["datetime_to"] = df["datetime_to"].str.replace(r"(\d{4}-\d{2}-\d{2}) 24:00:00", r"\1 23:59:59", regex=True)

    # save processed data to a .csv file
    save_directory = "api_service/sensor_data/batch_data"
    date_obj = date.today() - timedelta(days=1)
    date_str = date_obj.strftime("%Y%m%d")
    file_name = f"batch_data_{date_str}.csv"
    file_path = os.path.join(save_directory, file_name)
    os.makedirs(save_directory, exist_ok=True)

    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, mode='a', index=False, header=False)


def batch_process():
    """
    Calls the APIs for metadata and sensor data and performs a batch process.

    - Fetches metadata from the API to retrieve relevant station IDs.
    - Iterates through each station ID to request sensor data for a specified time range.
    - Saves the sensor data to a CSV file, organizing it by day.

    Function should be scheduled or executed periodically to keep sensor data updated.
    """

    # set global parameters
    headers_data = {"accept": "application/json"}

    #### metadata ####
    print(f"---- START FETCHING METADATA ON COMPONENTS ---- ")

    # set parameters for metadata api call
    api_url_components = "https://www.umweltbundesamt.de/api/air_data/v3/components/json"
    language_parameter = "en"
    index_parameter = "id"

    # perform api call
    fetch_components_metadata(language_parameter, index_parameter, api_url_components, headers_data)

    #### sensor data ####
    print(f"---- START FETCHING SENSOR DATA FOR DIFFERENT STATIONS ---- ")

    station_id_list = [
        "DEHH082" , "DEHH068", "DEHH008", "DEHH064"
        # , "DEHH015", "DEHH081", "DEHH026", "DEHH070", "DEHH079", "DENI063",
        # "DESH022", "DESH056", "DESH057", "DESH008", "DESH015", "DESH053", "DESH027", "DESH023", "DESH058", "DENW134",
        # "DENW021", "DENW024", "DENW208", "DENW112", "DENW043", "DENW188", "DENW071", "DEBE051", "DEBE010", "DEBE032",
        # "DEBE061", "DEBE069", "DEBE125", "DEBE056", "DEBE127", "DEBE126", "DEBE034", "DEBE065", "DEBE068", "DENW212"
    ]

    # set parameters for sensor data api call
    api_url = "https://www.umweltbundesamt.de/api/air_data/v3/airquality/json"

    # fetches data from previous day once a day and saves it in a file for all stations
    # is triggered by crontab / cronjob, which is executed once a day
    for station_id in station_id_list:
        date_from = date.today() - timedelta(days=1)
        time_from = 1
        date_to = date_from
        time_to = 24

        print(f"Fetching data for station: {station_id}")
        json_data = fetch_data(station_id, date_from, time_from, date_to, time_to, headers_data, api_url)

        if json_data:
            process_batch_data(json_data)
