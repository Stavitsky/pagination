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


def get_marker_record_query(sort_keys):
    query = 'SELECT {}, id FROM users WHERE id=%s;'
    selected_rows = ','.join(sort_keys)
    query = query.format(selected_rows)
    print query
    return query


def get_scalar_compare_query(keys, dirs, marker):
    cmp_signs = {'asc': '>', 'desc': '<'}
    if len(keys) != len(dirs) != len(marker):
        raise Exception
    result = []
    values = []
    for n in range(len(keys)):
        compare = ['%s=%%s' % k for k in keys[:n]]
        values.extend(marker[:n])
        compare.append('%s%s%%s' % (keys[n], cmp_signs[dirs[n]]))
        values.append(marker[n])
        result.append('(%s)' % ' and '.join(compare))
    query = '(%s)' % ' or '.join(result)
    return query, values


def get_order_query(pagination):
    values = []
    query = ' ORDER BY'
    if pagination.sort_keys and pagination.sort_dirs:
        for i in range(0, len(pagination.sort_keys)):
            query += ' %s %s,'
            values.append(pagination.sort_keys[i])
            values.append(pagination.sort_dirs[i])

    query += ' id %s'
    values.append(pagination.primary_sort_dir)
    return query, values


def get_main_query(pagination):
    scalar_values = []
    order_values = []
    pagination_query = ''
    order_query = ''

    keys = pagination.sort_keys
    dirs = pagination.sort_dirs
    keys.append('id')
    dirs.append(pagination.primary_sort_dir)
    print keys, dirs

    main_query = ('SELECT * FROM users')
    print main_query

    if pagination.marker_value:
        # pagination query part (scalar compare)
        pagination_query = ' WHERE '
        scalar_query, scalar_values = get_scalar_compare_query(
            keys, dirs, pagination.marker_value)
        pagination_query += scalar_query
        print pagination_query, scalar_values

    # order by query part
    order_query, order_values = get_order_query(pagination)

    order_values = [AsIs(v) if isinstance(v, basestring) else v
                    for v in order_values]

    print order_query, order_values

    values = scalar_values + order_values

    main_query = ''.join([main_query, pagination_query, order_query])

    if pagination.limit:
        main_query += ' LIMIT %s'
        values.append(pagination.limit)

    return main_query, values


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
        pagination = Pagination(limit=5,
                                primary_sort_dir='asc',
                                sort_keys=['first_name', 'last_name'],
                                sort_dirs=['asc', 'desc'],
                                marker_value=16)

        if pagination.marker_value:
            # get marker object from db
            marker_query = get_marker_record_query(pagination.sort_keys)
            cur.execute(marker_query, (str(pagination.marker_value),))
            marker_record = cur.fetchone()
            pagination.marker_value = marker_record

        query, values = get_main_query(pagination)

        cur.execute(query, values)
        resp = cur.fetchall()
        print_response(resp)

    finally:
        if con:
            con.close()

if __name__ == '__main__':
    main()
