"""Defines trends calculations for stations"""
import logging

import faust

#from dataclasses import dataclass
#from Type import List

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
#@dataclass(frozen=True)
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
#@dataclass(frozen=True)
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")


topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1",
                      key_type=str,
                      value_type=TransformedStation,
                      partitions=1)

table = app.Table(
    "org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


async def stations_streams_transformer(stations):
    async for station in stations:
        transformedStation = TransformedStation()
        transformedStation.station_id = station.station_id
        transformedStation.station_name = station.station_name
        transformedStation.order = station.order
        
        if station.red == True:
            transformedStation.line = 'red'
        elif station.blue == True:
            transformedStation.line = 'blue'
        elif station.green == True:
            transformedStation.line = 'green'
        else:
            logger.debug("No line colour was set for the station")
            continue
        
        table[station.station_id] = transformedStation
        
        await out_topic.send(key=station.station_name, value=transformedStation)

#


if __name__ == "__main__":
    app.main()
