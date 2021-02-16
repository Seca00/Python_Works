from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import time, sys
import json


topic_name = "StaticAnalyzeOutput"
kafka_server = "localhost:9202"
elastic_server =  {"host": "localhost", "port": "9200"}

es = Elasticsearch([elastic_server])

# To consume messages
consumer = KafkaConsumer(topic_name,
                         consumer_timeout_ms=1000,
                         api_version=(0, 10),
                         auto_offset_reset='earliest',
                         bootstrap_servers=[kafka_server])

esid = 0
index = "StaticAnalyzeOutput"
while True:
    for message in consumer:
        esid += 1
        msg = json.loads(message.value)
        print("Switching to Index", index)
        if not es.indices.exists(index=index):
            es.indices.create(index=index)
        es.indices.put_mapping(index=index, doc_type=msg['doc_type'], body=msg['schema']['mappings'])

        es.index(index=index, doc_type="static_analyze", id=esid, body=msg)

    # if no message sleep for 5 sec
    time.sleep(5)
