import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pycds import Obs
from crmprtd import insert

logging.basicConfig(level="DEBUG")

connection_string = "postgresql://hiebert@monsoon.pcic/crmp"

engine = create_engine(connection_string)
sesh = Session(bind=engine)

observations = (
    Obs(time="2021-03-01", datum=0, history_id=1000000, vars_id=1000000),
    Obs(time="2021-03-02", datum=0, history_id=1000000, vars_id=1000000),
    Obs(time="2021-03-03", datum=0, history_id=1000000, vars_id=1000000),
    Obs(time="2021-03-04", datum=0, history_id=1000000, vars_id=1000000),
    Obs(time="2021-03-05", datum=0, history_id=1000000, vars_id=1000000),
)

results = insert(sesh, observations, 100)

print(results)

