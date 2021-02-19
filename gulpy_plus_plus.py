import glob
from itertools import islice
import logging
from contextlib import contextmanager

from dateutil.parser import parse as date_parse
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pycds import Obs
from crmprtd.insert import insert

logging.basicConfig(level="DEBUG")

def convert_line_to_pycds_obs(line):
    history_id, date_time, datum, vars_id = line.split(',')
    return Obs(time=date_parse(date_time), datum=datum, history_id=history_id, vars_id=vars_id)

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


fname = glob.glob("/home/cdmb/PRISM/station_data_for_upload/ASP/*")[0]

with open(fname) as f:
    # Skip the header line
    lines = [line for line in islice(f, 1, None)]
    obs = [convert_line_to_pycds_obs(line) for line in lines]

connection_string = "postgresql://hiebert@monsoon.pcic.uvic.ca/crmp"

with transaction_to_rollback(connection_string) as sesh:
    results = insert(sesh, obs, 100)
    print(results)
