import os
import boto3
import awswrangler as wr
from pprint import pprint
from utils.constants import config_vars
from utils.logic import get_data_formated
from utils.wrap_datetimes import gen_intervales_date_day, convert_dt_to_timestamp

session = boto3.Session(aws_access_key_id=config_vars.get('AWS_ACCESS_KEY_ID'),\
                        aws_secret_access_key=config_vars.get('AWS_SECRET_ACCESS_KEY'))


CRYPTO_TO_INGEST = "dogecoin"

intervals = gen_intervales_date_day("2023-08-01", "2023-08-25", "5d")

df = get_data_formated(CRYPTO_TO_INGEST, intervals)

# to parquet using aws wrangler

df.to_parquet("./cryptos_prices/", compression="snappy",\
            index=False,\
            partition_cols=["name", "year", "month", "day", "hour"],
            engine="pyarrow")

# wr.s3.to_parquet(df,
#                  path=f"s3://{config_vars.get('BUCKET_NAME')}/cryptos_prices/",
#                  dataset=True,
#                  mode="overwrite_partitions",
#                  partition_cols=["name", "year", "month", "day", "hour"],
#                  boto3_session=session)