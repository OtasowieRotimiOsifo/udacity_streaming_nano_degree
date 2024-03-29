"""Contains functionality related to Stations"""
import json
import logging


logger = logging.getLogger(__name__)
logger.setLevel(10)

class Station:
    """Defines the Station Model"""

    def __init__(self, station_id, station_name, order):
        """Creates a Station Model"""
        self.station_id = station_id
        self.station_name = station_name
        self.order = order
        self.dir_a = None
        self.dir_b = None
        self.num_turnstile_entries = 0

    @classmethod
    def from_message(cls, value):
        """Given a Kafka Station message, creates and returns a station"""
        #logger.info("value: %s", value)
        return Station(value["station_id"], value["station_name"], value["order"])

    def handle_departure(self, direction):
        """Removes a train from the station"""
        logger.info("direction: %s", direction)
        if direction == "a":
            self.dir_a = None
        else:
            self.dir_b = None

    def handle_arrival(self, direction, train_id, train_status):
        """Unpacks arrival data"""
        #logger.info("train_id: %s", train_id)
        status_dict = {"train_id": train_id, "status": train_status.replace("_", " ")}
        if direction == "a":
            self.dir_a = status_dict
        else:
            self.dir_b = status_dict

    def process_message(self, json_data):
        """Handles arrival and turnstile messages"""
        #logger.info("json_data: %s", json_data)
        self.num_turnstile_entries = json_data["COUNT"]
