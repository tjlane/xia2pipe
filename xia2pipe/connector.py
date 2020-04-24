#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "Luca Gelisio"
__contact__ = "luca.gelisio@cfel.de"
__version__ = "0.1"
__date__ = "3/2020"
__status__ = "beta"
__license__ = "GPL v3+"

from datetime import datetime
from mysql import connector
from numpy import ndarray


def get_single(query, crystal_id, run, field_name):
    if len(query) == 0:
        raise IOError('no {} in database for '
                      'crystal_id={}, run={}'.format(field_name, crystal_id, run))
    if len(query) > 1:
        print(query)
        raise IOError('found multiple {}`s in database for '
                      'crystal_id={}, run={}'.format(field_name, crystal_id, run))
    return query[0][field_name]


class SQL(object):

    def __init__(self, config):

        self.config = config
        self.connection = None
        return


    def __enter__(self):

        # connect to the database
        self.connect()

        return self


    def __exit__(self, exception_type, exception_value, traceback):
        if self.is_connected():
            self.connection.disconnect()
        return


    def is_connected(self):

        # connected once?
        if type(self.connection) != type(None):

            # still connected?
            if self.connection.is_connected():

                return True

        return False


    def connect(self, verbose=False):
        """ connect to the database
        """

        try:
            self.connection = connector.connect(**self.config)

        except connector.Error as err:

            if err.errno == connector.errorcode.ER_ACCESS_DENIED_ERROR:
                raise ValueError("Wrong user or password...")

            elif err.errno == connector.errorcode.ER_BAD_DB_ERROR:
                raise ValueError("Database does not exist...")

            else:
                raise ValueError(err)

        # be verbose
        if verbose:

            print("MySQL connector parameters")
            for ki, vi in self.config.items():

                if ki == "password":
                    vi = "*****"

                print("  {}: {}".format(ki, vi))

        return


    def execute(self, query, dictionary=True, verbose=False):
        """ execute a query
        by default the query returns a dictionary
        """

        if verbose:
            print("{}: MySQL: {}".format(datetime.now().time(), query))

        # auto-connect
        if not self.is_connected():
            self.connect()

        cursor = self.connection.cursor(dictionary=dictionary)
        cursor.execute(query)

        return cursor


    @property
    def tables(self):
        """ get tables
        """
        query = "SHOW TABLES"

        cursor = self.execute(query, dictionary=False)
        result = [i[0] for i in cursor.fetchall()]

        cursor.close()

        return result


    def describe(self, table, view="dictionary", verbose=False):
        """ describe a table
        The view is either a dictionary or a list
        """
        query = "DESCRIBE {}".format(table)

        if verbose:
            print("{}: MySQL: {}".format(datetime.now().time(), query))

        cursor = self.execute(query)
        result = cursor.fetchall()

        cursor.close()

        if view == "dictionary":
            return result

        elif view == "list":

            return (
                [di["Field"] for di in result],
                [di["Type"] for di in result],
                [di["Null"] for di in result],
                [di["Key"] for di in result],
                [di["Default"] for di in result],
                [di["Extra"] for di in result],
            )

        else:
            raise ValueError("unknown view {}...".format(view))
            return


    def select(self, key, table, condition, verbose=False):
        """ return item in a given table satisfying a certain condition
        
        example:
            key [string or array] = 'crystal_id', ['crystal_id', 'crystal_id'] or '*'
            table [string or array] = 'Crystals' or ['Crystals', 'Repeated_diffraction']
            condition [dictionary] = {'metadata': "p10l2"}
        """

        # key and table are arrays
        arraylike = [list, tuple, ndarray]

        if type(key) not in arraylike:
            key = [key]

        if type(table) not in arraylike:
            table = [table]

        # item to select and table
        bkey, btable = "", ""

        for ki in key:
            bkey += "{}, ".format(ki)
        key = bkey[:-2]

        for ki in table:
            btable += "{}, ".format(ki)
        table = btable[:-2]

        # condition
        bcondition = ""

        for ki, vi in condition.items():
            if type(vi) == str and vi != "NULL":
                bcondition += '{}="{}" AND '.format(ki, vi)

            else:
                bcondition += "{}={} AND ".format(ki, vi)

        condition = bcondition[:-5]

        # execute the query
        query = "SELECT {} FROM {} WHERE {}".format(key, table, condition)

        if verbose:
            print("{}: MySQL: {}".format(datetime.now().time(), query))

        cursor = self.execute(query)
        result = cursor.fetchall()

        cursor.close()

        return result


    def insert(self, table, data, verbose=False):
        """ insert data into a table
        data is a dictionary, eg.  
        """

        key = "INSERT INTO {} (".format(table)
        value = "values ("

        for ki, vi in data.items():

            key += "{}, ".format(ki)

            if type(vi) == str and vi != "NULL":
                value += '"{}", '.format(vi)

            else:
                value += "{}, ".format(vi)

        key = "{})".format(key[:-2])
        value = "{})".format(value[:-2])

        # execute the query
        query = "{} {}".format(key, value)

        if verbose:
            print("{}: MySQL: {}".format(datetime.now().time(), query))

        cursor = self.execute(query)
        #result = cursor.fetchall()

        cursor.close()

        return #result


