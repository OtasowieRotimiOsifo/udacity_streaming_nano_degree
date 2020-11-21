"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

import sys
import os

from enum import IntEnum

cwd = os.getcwd()
sys.path.append(cwd)

from models.turnstile import Turnstile
from models.producer import Producer
from models.train import Train


logger = logging.getLogger(__name__)
logger.setLevel(10)

class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, 
                 station_id: int, 
                 name: str, 
                 color: IntEnum, 
                 direction_a=None, 
                 direction_b=None):
        self.name = name
        self.station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        
        topic_name = "org.chicago.cta.station.arrivals.monitor" 
        super().__init__(
            topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema, 
            num_partitions=1, #single node broker
            num_replicas=1 #number of brokers available in the work space
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)
        self.topic_name = topic_name

    def run(self, train: Train, 
            direction: str, 
            prev_station_id: int, 
            prev_direction: str) -> None:
        """Simulates train arrivals at this station"""
        
        logger.info("arrival of train: " + train.train_id + " at station: " + self.name + " topic name: " + self.topic_name + " line: " + self.color.name)
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                "station_id": self.station_id,
                "train_id": train.train_id,
                "direction": direction,
                "line": self.color.name,
                "train_status": train.status.name,
                "prev_station_id": prev_station_id,
                "prev_direction": prev_direction
            }
        )

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train: Train, 
                 prev_station_id: int, 
                 prev_direction: str) -> None:
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train: Train, 
                 prev_station_id: int, 
                 prev_direction: str) -> None:
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
