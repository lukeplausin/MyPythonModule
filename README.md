# MyPythonModule
My own personal module. Just stuff which I find useful.

##

## Elasticsearch shortcuts
Connect to an AWS managed elasticsearch cluster with the python API:

```
from lp.esutils import ConnectToElasticSearch

es_client = ConnectToElasticSearch('my-search-cluster.amazonaws.com')

print( es_client.info() )
```
