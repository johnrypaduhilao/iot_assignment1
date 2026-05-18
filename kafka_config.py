import os
import json
import random
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class KafkaConfig:
    def __init__(self):
        self.api_key = os.getenv("CONFLUENT_KEY")  
        self.secret_key = os.getenv("CONFLUENT_SECRET")
        self.bootstrap_server = os.getenv("CONFLUENT_SERVER")
    
    def set_config(self):
        KAFKA_CONFIG = {
            "bootstrap.servers": self.bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms":   "PLAIN",
            "sasl.username":     self.api_key,
            "sasl.password":     self.secret_key ,
        }

        return KAFKA_CONFIG
    
    def create_topic(self, topic_name, partitions=3, replication=3):
        admin  = AdminClient(self.set_config())
        result = admin.create_topics([NewTopic(topic_name, num_partitions=partitions,
                                            replication_factor=replication)])
        for t, future in result.items():
            try:
                future.result()
                print(f"Topic '{t}' created  ({partitions} partitions)")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"Topic '{t}' already exists — skipping")
                else:
                    raise

        print("Imports and helpers ready")


