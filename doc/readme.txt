# Stock Analysis Project - Data Schema

## Transaction Table

| Column Name | Data Type |
|---|---|
| transaction_id | STRING |
| symbol | STRING |
| price | FLOAT64 |
| volume | INT64 |
| trade_value | FLOAT64 |
| trade_type | STRING |
| trader_id | STRING |
| exchange | STRING |
| region | STRING |
| timestamp | TIMESTAMP |
| ingestion_time | TIMESTAMP |
| processing_latency_ms | INT64 |
| is_anomaly | BOOLEAN |
| anomaly_reason | STRING |

## Anomaly Table

| Column Name | Data Type |
|---|---|
| transaction_id | STRING |
| symbol | STRING |
| price | FLOAT64 |
| previous_avg_price | FLOAT64 |
| deviation_percent | FLOAT64 |
| anomaly_type | STRING |
| detected_at | TIMESTAMP |
