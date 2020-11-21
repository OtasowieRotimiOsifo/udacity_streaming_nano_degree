"""Producer base-class providing common utilites and functionality"""
import logging
import time
import socket

#from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""
 
    # Tracks existing topics across all Producer instances
    existing_topics = set(list())
    client_id = socket.gethostname()
    
    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1, #single node broker?
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081",
            "client.id": self.client_id
        }

        # If the topic does not already exist, try to create it
        #print(len(self.existing_topics))
        if self._topic_exists(self.topic_name) == False:
            #print(self.topic_name)
            ret = self.create_topic()
            if ret == 1:
                #print(len(self.existing_topics))
                Producer.existing_topics.add(self.topic_name)
    
        self.producer = AvroProducer(self.broker_properties, 
                                     default_key_schema=self.key_schema,
                                     default_value_schema=self.value_schema)
        
    def _topic_exists(self, topic_name: str) -> bool:
         if topic_name in Producer.existing_topics:
            return True
         return False
            
    def create_topic(self) -> int:
        """Creates the producer topic if it does not already exist"""
        try:
            client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
            topic_metadata = client.list_topics(timeout=200)
            topic_check = self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))
            if  topic_check == True:
                 return -1
             
            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas
                        #config={
                           # "cleanup.policy": "delete",
                          #  "compression.type": "lz4",
                          #  "delete.retention.ms": "2000",
                         #   "file.delete.delay.ms": "2000",
                        #}
                    )
                ]
            )
            status = 1
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.debug("topic created")  
                except Exception as e:
                    logger.fatal(f"failed to create topic {self.topic_name}: {e}")
                    status = -1
        except Exception as ex:
            logger.warn(f"Error in create topic {self.topic_name}: {ex}")
            status = -1
        return status

    def time_millis(self) -> int:
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            self.producer.flush()

