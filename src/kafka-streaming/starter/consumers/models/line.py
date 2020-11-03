"""Contains functionality related to Lines"""
import json
import logging

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        if value["line"] != self.color:
            return
        self.stations[value["station_id"]] = Station.from_message(value)

    def _handle_arrival(self, message):
        """Updates train locations"""
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station")
        else:
            logger.debug(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("unable to handle message due to missing station")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )

    def __get_message_topic(self, message) -> str:
        return message.topic()
    
    def __is_in_topic(self, message, topic_name: str) -> bool:
        message_topic = self.__get_message_topic(message)
        if topic_name in message_topic:
            return True
        return False
    
    def process_message(self, message):
        """Given a kafka message, extract data"""
    
        if self.__is_in_topic(message, 'org.chicago.cta.transformed.stations') == True: # Set the conditional correctly to the stations Faust Table
            try:
                value = json.loads(message.value())
                self._handle_station(value)
            except Exception as e:
                logger.fatal("bad station? %s, %s", value, e)
        elif self.__is_in_topic(message, 'cta.station.arrivals') == True: # Set the conditional to the arrival topic
            self._handle_arrival(message)
        elif  self.__is_in_topic(message, 'TURNSTILE_SUMMARY') == True: # Set the conditional to the KSQL Turnstile Summary Topic
            json_data = json.loads(message.value())
            station_id = json_data.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.debug("unable to handle message due to missing station")
                return
            station.process_message(json_data)
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic
            )