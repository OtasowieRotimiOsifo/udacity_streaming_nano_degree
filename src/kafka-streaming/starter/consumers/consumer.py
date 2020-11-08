"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    BROKER_URL = "PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        
        group_id = topic_name_pattern
        self.broker_properties = {
            'bootstrap.servers': self.BROKER_URL,
            "group.id": group_id,
            'default.topic.config': {'auto.offset.reset': 'earliest'}
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = self.SCHEMA_REGISTRY_URL 
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #self.consumer.subscribe(pattern=topic_name_pattern, on_assign=self.on_assign)
        topics = [topic_name_pattern]
        self.consumer.subscribe(topics, on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        
        for partition in partitions:
          if self.offset_earliest == True:
              partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
             message = self._consumer.poll(self.consume_timeout)
             
             if message is None:
                 return 0;
             elif not message.error():
                 self.message_handler(message) 
                 return 1
             else:
                 logger.error("Kafka error: %s", message.error().str())
                 raise confluent_kafka.KafkaException(message.error())
        except Exception as e:
            print('An exception of type: ', type(e), ' occured')
            return 0
        except SerializerError as e:
            print('An exception of type: ', type(e), ' occured')
            return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
