import sys
from itertools import islice
import logging
from contextlib import contextmanager
import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pycds import Obs
from crmprtd.insert import insert

logging.basicConfig(level="DEBUG")

@contextmanager
def transaction_to_rollback(connection_string):
    engine = create_engine(connection_string)
    connection = engine.connect()
    transaction = connection.begin()
    sesh = Session(bind=connection)
    yield sesh
    sesh.close
    transaction.rollback()
    connection.close()


for fname in sys.argv[1:]:
    with open(fname) as csvfile:
        fieldnames = ("history_id", "time", "datum", "vars_id")
        reader = csv.DictReader(csvfile, fieldnames)
        obs = [Obs(**row) for row in islice(reader, 1, None)]

connection_string = "postgresql://hiebert@monsoon.pcic.uvic.ca/crmp"

with transaction_to_rollback(connection_string) as sesh:
    results = insert(sesh, obs, 100)
    print(results)
