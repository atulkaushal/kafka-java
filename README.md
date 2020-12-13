# kakfa-java

Steps to configure elastic search:

	1. Download elastic search from https://www.elastic.co/downloads/elasticsearch
	2. Unzip installable.
	3. Set ELASTIC_HOME and PATH.
	4. Start using command `elasticsearch.bat`
	5. Verify by accessing http://localhost:9200/ using postman or cURL.
	

Few more Elastic commands:

	1. GET http://localhost:9200/_cat/health?v
	2. GET http://localhost:9200/_cat/nodes?v
	3. GET http://localhost:9200/_cat/indices?v
	4.	PUT http://localhost:9200/twitter								//Creates a document
	5. PUT http://localhost:9200/twitter/tweets/1					
			{
    			"course": "Kafka for Beginners",
    			"module": "Elastic search"
			}
	6. GET http://localhost:9200/twitter/tweets/1
	7. DELETE http://localhost:9200/twitter
	8. GET http://localhost:9200/twitter/_doc/uV14WXYB0uHI-27wpSVF   //Get by Id.
	