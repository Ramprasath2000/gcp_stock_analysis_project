import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level = logging.INFO)

PROJECT_ID = "stock-pipeline-project-499806"
SUBSCRIPTION = "projects/stock-pipeline-project-499806/subscriptions/stock-transactions-topic-sub"
BQ_TABLE = "stock-pipeline-project-499806.stock_analytics.transactions"
GCS_BUCKET = "gs://stock-pipeline-bucket-499806"


# Define a function to parse the incoming Pub/Sub messages
class ParseMessage(beam.DoFn):
    def process(self, message):
        try:
            record = json.loads(message.decode("utf-8"))
            yield record
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")


#Define a function to clean the data

class CleanData(beam.DoFn_):
    def process(self, record):
        required_fields = ["transaction_id", "symbol", "price", "volume", "trade_type",
                           "trader_id", "exchange", "region", "timestamp" ]
        
        # check all required fields are present
        for field in required_fields:
            if field not in record:
                logging.warning(f"Dropping record missing filed: {field}")
                return
        
        #Validate price and volume are positive
        if record["price"] <=0 or record["volume"] <=0:
            logging.warning(f"Dropping record with non-positive price or volume")
            return
        yield record
