from fetch_data import batch_process
from stream_simulation import load_batch_data, simulate_stream, periodic_delete
import threading
import logging
import os


# ----------------------------------------------------------------------------------------------------------------------
# SET UP CONTAINER-SPECIFIC LOGGING
# ----------------------------------------------------------------------------------------------------------------------

# creating log file
log_path = '/usr/src/api_service'
log_filename = os.path.join(log_path, 'api_service_logs.log')

# logger
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)


# ----------------------------------------------------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("---- START BATCH PROCESS ---- ")
    batch_process()
    logger.info("---- END BATCH PROCESS ---- ")

    logger.info("---- START STREAM SIMULATION ---- ")
    df = load_batch_data()
    stream_thread = threading.Thread(target=simulate_stream, args=(df,))
    stream_thread.start()

    logger.info("---- START DELETING OLD FILES ---- ")
    delete_thread = threading.Thread(target=periodic_delete, daemon=True)
    delete_thread.start()

    stream_thread.join()

    logger.info("---- FINISHED STREAM SIMULATION PROCESSES ---- ")
