#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Example of using pymar with relational data bases.
"""
import os

import sqlalchemy
from sqlalchemy.sql import text

from pymar.datasource import DataSourceFactory
from pymar.plugins.datasources.SQLDataSource import SQLDataSource
from pymar.producer import Producer


class SimpleProducer(Producer):
    """Producer for the task of sum of squares of values
    For this particular task the keys are not really needed, but you still need to process them.
    """
    WORKERS_NUMBER = 4

    @staticmethod
    def map_fn(data_source):
        for val in data_source:
            yield val**2

    @staticmethod
    def reduce_fn(data_source):
        return sum(data_source)


class SimpleSQLSource(SQLDataSource):
    """Data source for the task of sum of squares of values.
    Illustrates work with SimpleSQLSource.
    You just have to provide the path to database, name of the table and list of names of needed columns.
    If you need all the columns, set COLUMNS = ["*"]

    If you use SQLite, make sure that your workers have access to the file of database.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    CONF = 'sqlite:///exampledb'
    TABLE = "examples"
    COLUMNS = [
        "value",
    ]


def init_database():
    print "Initialize database"

    CREATE_TABLE = text("""
    CREATE TABLE IF NOT EXISTS examples (
            id INTEGER,
            value INTEGER,
            PRIMARY KEY(id)
    );
    """)

    INSERT_DATA = text("INSERT INTO examples(id, value) VALUES (:id, :value);")

    engine = sqlalchemy.create_engine(SimpleSQLSource.CONF, echo=False)
    engine.execute(CREATE_TABLE)
    engine.execute(INSERT_DATA, [
        {
            "id": i,
            "value": i
        }
        for i in range(10**7)
    ])
    print "Database initialized."


def remove_database():
    print "Remove database"
    os.remove("./exampledb")
    print "Database removed."

if __name__ == "__main__":
    """
    Before starting this script launch corresponding workers:
    worker.py -f ./squaredsum_sql.py -s SimpleSQLSource -p SimpleProducer -q 127.0.0.1 -w 4
    (Run workers from the same directory because they need to use the same sqlite database.
    Otherwise you have to set the full path in CONF variable.)
    """
    init_database()

    producer = SimpleProducer()
    factory = DataSourceFactory(SimpleSQLSource)
    value = producer.map(factory)
    print "Answer: ", value

    remove_database()