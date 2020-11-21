"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def __get_weather_from_message(self, message):
        return message.value()

    def process_message(self, message):
        """Handles incoming weather data"""
        weather = message.value() #self.__get_weather_from_message(message)

        self.temperature = weather.get("temperature")
        self.status = weather.get("status")
