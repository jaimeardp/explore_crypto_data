import multiprocessing
import time
import boto3
import traceback
import concurrent.futures
from utils.constants import config_vars
from utils.controller import task_get_data

session = boto3.Session(aws_access_key_id=config_vars.get('AWS_ACCESS_KEY_ID'),\
                        aws_secret_access_key=config_vars.get('AWS_SECRET_ACCESS_KEY'))


cryptos_to_ingest = [
    {"name": "bitcoin", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "ethereum", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "dogecoin", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "cardano", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "polkadot", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "litecoin", "start_date": "2023-07-01", "end_date": "2023-07-15"},
    {"name": "celer-network", "start_date": "2023-07-01", "end_date": "2023-07-15"}
]

if __name__ == "__main__":
    try:
        
        fs = []
        manager = multiprocessing.Manager()

        queue = manager.Queue()

        queue.put(0)
        queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.put(4)

        start = time.time()

        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:

            for crypto_dict in cryptos_to_ingest:

                print("start process")

                fs.append(executor.submit(task_get_data,\
                                          crypto_dict.get('name'), 
                                          crypto_dict.get('start_date'), crypto_dict.get('end_date'), queue))

            for i, f in enumerate(concurrent.futures.as_completed(fs)):
                exce = f.exception()
                if exce:
                    print(f"Process {i} - error: {exce}")
                    raise ChildProcessError(f)
                else:
                    print(f"Process {i} - result {f} = {f.result()}")

        end = time.time()

        print(f"total time: {end - start}")
        
    except Exception as e:

        error = ''.join(traceback.format_exception(None, e, e.__traceback__))

        print("error main process: ", error)

        traceback.print_exc()
        
        raise e