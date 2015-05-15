#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2.extensions import AsIs


class Pagination(object):

    """Class for pagination query."""

    def __init__(self, limit=None, primary_sort_dir='desc', sort_keys=None,
                 sort_dirs=None, marker_value=None):
        """This puts all parameters used for paginate query together.
        :param limit: Maximum number of items to return;
        :param primary_sort_dir: Sort direction of primary key.
        :param marker_value: Value of primary key to identify the last item of
                             the previous page.
        :param sort_keys: Array of attributes passed in by users to sort the
                            results besides the primary key.
        :param sort_dirs: Per-column array of sort_dirs, corresponding to
                            sort_keys.
        """
        self.limit = limit
        self.primary_sort_dir = primary_sort_dir
        self.marker_value = marker_value
        self.sort_keys = sort_keys or []
        self.sort_dirs = sort_dirs or []


def create_sql_query(pagination):
    pagination_query = ''
    values = []
    main_query = ('SELECT * FROM users')
    if pagination.marker_value:
        if pagination.primary_sort_dir == 'desc':
            pagination_query += ' WHERE id < %s'
        else:
            pagination_query += ' WHERE id > %s'
        values.append(pagination.marker_value)

    pagination_query += ' ORDER BY'

    if pagination.sort_keys and pagination.sort_dirs:
        for i in range(0, len(pagination.sort_keys)):
            # Note (alexstav): all sort keys/dirs except first
            # should have comma
            if i != 0:
                pagination_query += ','
            pagination_query += ' %s %s'
            values.append(pagination.sort_keys[i])
            values.append(pagination.sort_dirs[i])

    pagination_query += ', id %s'
    values.append(pagination.primary_sort_dir)

    if pagination.limit:
        pagination_query += ' LIMIT %s'
        values.append(pagination.limit)

    result_query = main_query + pagination_query + ';'
    return result_query, values


def print_response(resp):
    print "--------------RESPONSE------------"
    for r in resp:
        print '{}'.format(r)
    print "----------------------------------"    


def main():
    con = None
    try:
        con = psycopg2.connect('dbname=pagination user=alexstav')
        cur = con.cursor()
        pagination = Pagination(limit=10,
                                primary_sort_dir='asc',
                                sort_keys=['birth_date'],
                                sort_dirs=['desc'],
                                marker_value=5)
        query, values = create_sql_query(pagination)

        # Note (alexstav): AsIs need for insert string value
        # into string without quotes
        print "values:\n{}".format(values)
        values = [AsIs(v) if isinstance(v, basestring) else v
                  for v in values]
        print "values_asis:\n{}\nquery:\n{}".format(values, query)

        cur.execute(query, values)
        resp = cur.fetchall()
        print_response(resp)
    finally:
        if con:
            con.close()

if __name__ == '__main__':
    main()
