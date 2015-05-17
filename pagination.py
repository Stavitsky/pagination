#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
from psycopg2.extensions import AsIs


class MarkerNotFound(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


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


def limited_by_marker_and_limit(resp, marker_value, limit):
    start_index = 0
    if not limit:
        limit = len(resp)
    if marker_value:
        start_index = _get_start_index(resp, marker_value)
    else:
        marker_value = 0
    return resp[start_index:start_index+limit]


def _get_start_index(resp, marker_value):
    start_index = -1
    for i, record in enumerate(resp):
        if record[0] == marker_value:
            start_index = i + 1
            break
    if start_index < 0:
        raise MarkerNotFound("Marker {} not found!".format(marker_value))
    return start_index


def create_sql_query(pagination):
    values = []
    main_query = ('SELECT * FROM users')
    pagination_query = ' ORDER BY'
    if pagination.sort_keys and pagination.sort_dirs:
        for i in range(0, len(pagination.sort_keys)):
            pagination_query += ' %s %s,'
            values.append(pagination.sort_keys[i])
            values.append(pagination.sort_dirs[i])

    pagination_query += ' id %s'
    values.append(pagination.primary_sort_dir)
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
        pagination = Pagination(limit=None,
                                primary_sort_dir='asc',
                                sort_keys=['first_name', 'last_name'],
                                sort_dirs=['asc', 'asc'],
                                marker_value=None)
        query, values = create_sql_query(pagination)

        # Note (alexstav): AsIs need for insert string value
        # into string without quotes
        values = [AsIs(v) if isinstance(v, basestring) else v
                  for v in values]

        cur.execute(query, values)
        resp_all = cur.fetchall()

        resp_limited = limited_by_marker_and_limit(resp_all,
                                                   pagination.marker_value,
                                                   pagination.limit)

        print_response(resp_limited)
    finally:
        if con:
            con.close()

if __name__ == '__main__':
    main()
