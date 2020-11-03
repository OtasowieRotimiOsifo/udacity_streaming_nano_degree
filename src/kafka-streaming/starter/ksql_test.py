#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 22:05:58 2020

@author: otasowie
"""
import logging
from ksql import KSQLAPI
logging.basicConfig(level=logging.DEBUG)
KSQL_URL = 'http://localhost:8088'
client = KSQLAPI(KSQL_URL)
''
#create_tunsrstile_query = "CREATE TABLE turnstilex (station_id INTEGER, \
#    station_name VARCHAR, \
#    line VARCHAR) WITH (KAFKA_TOPIC='org.chicago.cta.turnstiles',\
#                        VALUE_FORMAT='AVRO',\
#                        KEY='station_id');"

#client.ksql(create_tunsrstile_query)
client.ksql('show tables')