import os
import time
import boto3
import duckdb as db
import pandas as pd
import awswrangler as wr
import streamlit as st
from dotenv import load_dotenv
from utils.constants import config_vars

load_dotenv()

# st.write("Hello world!")

session = boto3.Session(aws_access_key_id=os.environ["aws_access_key_id_me"],\
                        aws_secret_access_key=os.environ["aws_secret_access_key_me"])

duckdb_connection = db.connect(database=":memory:", read_only=False)

start = time.time()

df = wr.s3.read_parquet(f"s3://{config_vars.get('BUCKET_NAME')}/cryptos_prices/", 
                        dataset=True, 
                        use_threads=True,
                        boto3_session=session)

end = time.time()
print(f"read parquet ok - {end - start}")

duckdb_connection.query("CREATE TABLE cryptos_prices AS SELECT * FROM df")

print("query parquet ok")

duckdb_connection.table("cryptos_prices").show()

# st.line_chart(df, x="symbol", y="prices")
