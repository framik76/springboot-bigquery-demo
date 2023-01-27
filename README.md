# springboot-bigquery-demo
Springboot demo microservice with some api to get data using Google BigQuery services

### Requirements:
- GCP service account with BigQuery api enabled
- An instance of GCP Cloud SQL
- A BigQuery dataset (in the same region of the cloud sql instance)
- A BigQuery table in the dataset

### Environment variables:
- GCP_CREDENTIALS_ENCODED_KEY: The string encoded in base64 of the google service account JSON file 
- GCP_PROJECT_ID: The GCP project id
- GCP_BIGQUERY_DATASET_NAME: The bigquery dataset name
- GCP_BIGQUERY_TABLE_NAME: The bigquery table name
- GCP_SQL_REGION: The region of the GCP SQL instance to get data from 
- GCP_SQL_INSTANCE_ID: The GCP SQL instance id
- GCP_SQL_DB_NAME: The name of the SQL DB to get data from
- GCP_SQL_TABLE_NAME: The name of the SQL table to get data from

<a href="https://docs.google.com/presentation/d/18nZatJQa6zzUP61EUIznV5nWHHrySotkBgy4QladrCU/edit?usp=sharing">Presentation</a>