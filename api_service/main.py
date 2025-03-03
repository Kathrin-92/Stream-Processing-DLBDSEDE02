from fetch_data import batch_process
from stream_simulation import load_batch_data, simulate_stream

if __name__ == "__main__":
    print(f"---- START BATCH PROCESS ---- ")
    batch_process()
    print(f"---- END BATCH PROCESS ---- ")
    print(f"---- START STREAM SIMULATION ---- ")
    df = load_batch_data() # to do: probably make more fail-safe? should only load data that has not been processed yet
    simulate_stream(df)
    # to do: check if that works correctly - what if there is already a new batch-file but the stream is still running on the old file?
    # probably need to configure the cronjob accordingly: https://stackoverflow.com/questions/54804736/how-to-make-sure-one-cronjob-is-run-after-another-cronjob-is-completed#:~:text=A%20simple%20solution%20is%20to,time%20and%20then%20try%20again.
    # https://askubuntu.com/questions/1397551/how-can-i-ensure-that-a-task-is-finished-before-cron-starts-a-new-one
    print(f"---- END STREAM SIMULATION ---- ")
