# Script to download data
We have created this script to automatically download all the data with which we are going to work in the Transparency project. This script uses a spark streamin implementation and works with ReactiveX, MongoDB and Elastic Search. You can do multiple tasks that are explained later.

## Execution  tasks

### Spark tasks
#### Spark server
Launch the spark context server to wait the client and ingest the data to mongo:
```
python main.py server
```

#### Spark client
Launch the spark context client to send the data to spark server:
```
python main.py client
```

### Index tasks
#### Full index
This task cleans the actual index on the Elastic cluster and indexes all the licitations stored in the MongoDB Cluster. Also, creates the Elastic index if needed.
```
python main.py full-index
```

#### Update index
This task indexes the new licitations stored in the MongoDB cluster without wiping out the actual index.
```
python main.py update-index
```

#### Clean index
This task wipes out the actual index on Elastic cluster.
```
python main.py clean-index
```
