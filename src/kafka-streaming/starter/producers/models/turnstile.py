"""Creates a turnstile data producer"""
import sys
import os

import logging
from pathlib import Path

from confluent_kafka import avro

cwd = os.getcwd()
sys.path.append(cwd)

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")
    def __init__(self, station):
        """Create the Turnstile"""
        self.station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
                
        topic_name = "org.chicago.cta.station.turnstile" 
        super().__init__(
            topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema, 
            num_partitions=1,
            num_replicas=1
        )
       
        self.topic_name = topic_name
        station.station_name = self.station_name
        self.station = station
        self.turnstile_hardware = TurnstileHardware(self.station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        try:
            for _ in range(num_entries):
                self.producer.produce(
                                      topic=self.topic_name,
                                      key={"timestamp":  self.time_millis()},
                                      value={
                                             "station_id": self.station.station_id,
                                             "line": self.station.color.name,
                                             "station_name": self.station_name
                                            }
                )
      
        except Exception as e:
            logger.fatal("An exception of type: ", type(e), " occurred")
            raise(e)