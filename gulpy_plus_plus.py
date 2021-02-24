import sys
from itertools import islice
import logging
from contextlib import contextmanager
import csv
from optparse import OptionParser

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


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--connection-string", dest="connection_string",
                      help="database connection string",
                      default="postgresql://crmp@monsoon.pcic.uvic.ca/crmp"
                      )
    (options, filenames) = parser.parse_args()

    for fname in filenames:
        with open(fname) as csvfile:
            fieldnames = ("history_id", "time", "datum", "vars_id")
            reader = csv.DictReader(csvfile, fieldnames)
            obs = [Obs(**row) for row in islice(reader, 1, None)]

        with transaction_to_rollback(options.connection_string) as sesh:
            results = insert(sesh, obs, 100)
            print(results)
