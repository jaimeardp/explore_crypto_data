
class PipelineError(Exception):
    pass

_DELIVERY_STREAM_NAME = "DuckDB-Ingest"

def send_firehose_batch(data, firehose_client):
    try:
        response = firehose_client.put_record_batch(
            DeliveryStreamName=_DELIVERY_STREAM_NAME,
            Records=data
        )
        if response.get("FailedPutCount") > 0:
            raise PipelineError(f"Error in firehose response {response}")
    except Exception as e:
        print(e)
        raise e