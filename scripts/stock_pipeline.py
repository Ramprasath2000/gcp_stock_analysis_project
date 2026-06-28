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

#Define a function to enrich data

class EnrichData(beam.DoFn):
	def process (self, record):
		#Add trade value
		record["trade_value"] = round(record["price"] * record["volume"], 2)
		

		#Add ingestion time
		ingestion_time = datetime.now(timezone.utc).isoformat()
		record["ingestion_time"] = ingestion_time

		#Add processing latency
		event_time = datetime.fromisoformat(record["timestamp"])
		ingestion = datetime.fromisoformat(ingestion_time)
		latency = ingestion - event_time
		record["processing_latency_ms"] = int(latency.total_seconds() * 1000 )
	

		#aNAMOLY FIELDS - DEFAULT FOR NOW
		record["is_anamoly"] = False
		record["anomaly_reason"] = None

		yield record
		
#Defining the BigQuery schema for the output table

BQ__SCHEMA = {
     "fields": [
          {"name": "transaction_id", "type": "STRING"},
          {"name": "symbol", "type": "STRING"},
          {"name": "price", "type": "FLOAT"},
          {"name": "volume", "type": "INTEGER"},
          {"name": "trade_value", "type": "FLOAT"},
          {"name": "trade_type", "type": "STRING"},
          {"name": "trader_id", "type": "STRING"},
          {"name": "exchange", "type": "STRING"},
          {"name": "region", "type": "STRING"},
          {"name": "timestamp", "type": "TIMESTAMP"},
          {"name": "ingestion_time", "type": "TIMESTAMP"},
          {"name": "processing_latency_ms", "type": "INTEGER"},
          {"name": "is_anomaly", "type": "BOOLEAN"},
          {"name": "anomaly_reason", "type": "STRING"}
     ]
}


#Main pipeline function

def run():
     options = PipelineOptions()
     options.view_as(StandardOptions).streaming = True

     with beam.Pipeline(options = options) as p:
          (
               p
               | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription = SUBSCRIPTION)
               | "Parse JSON" >> beam.ParDo(ParseMessage())
               | "Clean Data" >> beam.ParDo(CleanData())
               | "Enrich Data" >> beam.ParDo(EnrichData())
               | "Write to BigQuery" >> WriteToBigQuery(
                    BQ_TABLE,
                    schema = BQ_SCHEMA,
                    write_disposition = BigQueryDisposition.WRITE_APPEND,
                    create_disposition = BigQueryDisposition.CREATE_IF_NEEDED
               )
          )

          if __name__ == "__main__";
               run()
