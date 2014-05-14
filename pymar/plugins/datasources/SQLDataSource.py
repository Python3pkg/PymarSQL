#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymar.datasource import DataSource


class SQLDataSource(DataSource):
    """Data source based on a table in data base.
    Your data are supposed to be in two columns of given table.
    To use, inherit from this class and set CONF, TABLE, KEY and VALUE.
    Key and value - names of columns with keys and values respectively.

    Be careful: if you use "localhost" in configuration, your workers will access local database
    on their host, not on the host of producer! If it is not what you want, use external IP-address.
    """

    REQUEST_TEMPLATE = """
    SELECT %s FROM %s
    LIMIT %d
    OFFSET %d;
    """

    COUNT_TEMPLATE = """
    SELECT count(*) FROM %s;
    """

    #Redefine in subclass
    CONF = "postgresql://login:password@127.0.0.1/exampledb"

    #Redefine in subclass
    TABLE = "example_table"

    #Redefine in subclass
    COLUMNS = [
        "id",
        "column1",
        "column2"
    ]

    def __init__(self, **kwargs):
        DataSource.__init__(self, **kwargs)
        import sqlalchemy
        self.engine = sqlalchemy.create_engine(self.CONF, echo=True)
        columns = " ".join(self.COLUMNS)
        print "Columns = ", columns
        self.cursor = self.engine.execute(self.REQUEST_TEMPLATE %
                                          (columns, self.TABLE, self.limit, self.offset))

    def __iter__(self):
        return self._get_cursor_data(self.cursor)

    @staticmethod
    def _get_cursor_data(cursor):
        while True:
            rows = cursor.fetchmany()
            if not rows:
                return
            for row in rows:
                if len(row) == 1:
                    yield row[0]
                else:
                    yield tuple(val for val in row)

    @classmethod
    def full_length(cls):
        import sqlalchemy
        engine = sqlalchemy.create_engine(cls.CONF, echo=True)
        cursor = engine.execute(cls.COUNT_TEMPLATE % (cls.TABLE,))
        return cursor.scalar()