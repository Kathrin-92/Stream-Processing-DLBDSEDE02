from fetch_data import batch_process
from stream_simulation import load_batch_data, simulate_stream, periodic_delete
import threading


if __name__ == "__main__":
    print(f"---- START BATCH PROCESS ---- ")
    batch_process()
    print(f"---- END BATCH PROCESS ---- ")

    print(f"---- START STREAM SIMULATION ---- ")
    df = load_batch_data()
    stream_thread = threading.Thread(target=simulate_stream, args=(df,))
    stream_thread.start()

    print(f"---- START DELETING OLD FILES ---- ")
    delete_thread = threading.Thread(target=periodic_delete, daemon=True)
    delete_thread.start()

    stream_thread.join()
