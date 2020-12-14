# KAFKA
Apache Kafka: A Distributed Streaming Platform.

This project contains multiple maven modules which shows how Kafka consumers, producers,connect and streams work.

	1. kafka-basics: 		Explains the basic operations of Kafka.
	2. kafka-connect:		How to leverage Kafka connect.
	3. kafka-consumer-elastic:	Consumes data using Kafka consumer and push it Elasticsearch.
	4. kafka-streams-filter-tweets:	Access information using Kafka Steams based on few conditions.
	5. kafka-twitter-producer:	Kafka producer to read data from twitter and put them into topics.

# how to install ElasticSearch

Steps to configure elasticsearch:

	1. Download elastic search from https://www.elastic.co/downloads/elasticsearch
	2. Unzip installable.
	3. Set ELASTIC_HOME and PATH.
	4. Start using command `elasticsearch.bat`
	5. Verify by accessing http://localhost:9200/ using postman or cURL.
	

Few Elastic commands:

	1. GET http://localhost:9200/_cat/health?v
	2. GET http://localhost:9200/_cat/nodes?v
	3. GET http://localhost:9200/_cat/indices?v
	4. PUT http://localhost:9200/twitter				//Creates a document
	5. PUT http://localhost:9200/twitter/tweets/1					
			{
    			"Project": "Kafka with Java",
    			"module": "Elastic search"
			}
	6. GET http://localhost:9200/twitter/tweets/1
	7. DELETE http://localhost:9200/twitter
	8. GET http://localhost:9200/twitter/_doc/uV14WXYB0uHI-27wpSVF   //Get by Id.
	
