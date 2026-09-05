"""
Simple script to create example indices in Elasticsearch.
Uses ELASTIC_URL env var or defaults to http://localhost:9200
"""
import os
import json
from elasticsearch import Elasticsearch

ES_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
es = Elasticsearch([ES_URL])

def create_index(name: str, mapping: dict):
    if es.indices.exists(index=name):
        print(f"Index {name} already exists")
        return
    es.indices.create(index=name, body=mapping)
    print(f"Created index {name}")

if __name__ == "__main__":
    # Minimal mapping example for loyalty-metrics-*
    metrics_mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "environment": {"type": "keyword"},
                "service.name": {"type": "keyword"},
                "metric.name": {"type": "keyword"},
                "metric.value": {"type": "double"},
                "api.name": {"type": "keyword"}
            }
        }
    }

    try:
        create_index("loyalty-metrics-000001", metrics_mapping)
    except Exception as e:
        print("Error creating index:", e)
        print("Ensure Elasticsearch is reachable at", ES_URL)
