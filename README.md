#   MongoDB Data Ingestion Using Python (Pymongo)

## Getting Started


1. Make sure you have MongoDB installed and running on your local machine or remote server.
2. Install the required Python packages:
``pip install pymongo
``
3. Clone the repository
4. modify the following values and in configuration in main variables and start the file

```
DAY_DIFFERENCE=1
TOTAL_THREADS_WHICH_INGEST_DATA = 10
NO_OF_EXECUTION_BY_THREAD=10
SIZE_OF_INSERT_RECORDS_IN_ONE_BATCH=100
DB_NAME="ts_old"
COLLECTION_NAME="ts_data_granular_hours"
```



