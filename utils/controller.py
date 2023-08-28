import os
import boto3
import traceback
import awswrangler as wr
from pprint import pprint
from utils.constants import config_vars
from utils.logic import get_data_formated
from utils.wrap_datetimes import gen_intervales_date_day

_session = boto3.Session(aws_access_key_id=config_vars.get('AWS_ACCESS_KEY_ID'),\
                        aws_secret_access_key=config_vars.get('AWS_SECRET_ACCESS_KEY'))

def task_get_data(crypto, start_date, end_date, queue):

    try:

        proxy_id = queue.get()

        intervals = gen_intervales_date_day(start_date, end_date, "5d")

        fh = _session.client("firehose", region_name="us-east-1")

        df = get_data_formated(crypto, intervals, proxy_id, fh)

        # wr.s3.to_parquet(df,
        #             path=f"s3://{config_vars.get('BUCKET_NAME')}/cryptos_prices/",
        #             dataset=True,
        #             mode="overwrite_partitions",
        #             partition_cols=["name", "year", "month", "day", "hour"],
        #             boto3_session=_session)
    except Exception as e:
        #print(e)
        error = ''.join(traceback.format_exception(None, e, e.__traceback__))

        #print("error main process: ", error)
        return "failed with proxy_id: " + str(proxy_id)
    finally:
        queue.put(proxy_id)

    return "ok"

    

# to parquet using aws wrangler

# df.to_parquet("./cryptos_prices/crypto_report.parquet",\
#                 compression="snappy",\
#                 index=False,\
#                 engine="pyarrow"
#             )