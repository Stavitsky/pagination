#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import random
from datetime import datetime

FIRST_NAMES = ['Alexandr', 'Vladimir', 'Nikolay', 'Denis', 'Nikita', 'Evgeniy',
               'Maksim', 'Dmitriy', 'Egor', 'Andrey', 'Ivan', 'Danila']
LAST_NAMES = ['Petrov', 'Ivanov', 'Ezjov', 'Stavitskiy', 'Chadin', 'Kruglov',
              'Smirnov', 'Sokolov', 'Novikov', 'Volkov', 'Vlasov', 'Moiseev']


def _generate_birthdate():
    year = random.randint(1950, 1998)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return datetime(year, month, day)


def generate_users(user_count):
    for i in range(0, user_count):
        user = {'first_name': random.choice(FIRST_NAMES),
                'last_name': random.choice(LAST_NAMES),
                'birth_date': _generate_birthdate(),
                'time_added': None}
        yield user


def create_db_model():
    con = None

    try:
        con = psycopg2.connect('dbname=pagination user=alexstav')
        cur = con.cursor()

        cur.execute('CREATE SEQUENCE user_id_seq;')
        cur.execute('CREATE TABLE users ('
                    ' id bigserial PRIMARY KEY,'
                    ' first_name text,'
                    ' last_name text,'
                    ' birth_date timestamp,'
                    ' time_added timestamp'
                    ');')
        cur.execute('ALTER TABLE users ALTER COLUMN id SET DEFAULT'
                    ' NEXTVAL(\'user_id_seq\');')
        con.commit()

    finally:
        if con:
            print con
            con.close()


def fill_db_table(user_count=1):
    con = None

    try:
        con = psycopg2.connect(database="pagination", user="alexstav")
        cur = con.cursor()

        user_ins = ('INSERT INTO users ('
                    'first_name, last_name, birth_date, time_added)'
                    ' VALUES (%s, %s, %s, %s)')

        for user in generate_users(user_count):
            cur.execute(user_ins, [user['first_name'],
                                   user['last_name'],
                                   user['birth_date'],
                                   datetime.now()])
            print user
            con.commit()

    finally:
        if con:
            con.close()


def main():
    # create_db_model()
    fill_db_table(user_count=10)

if __name__ == '__main__':
    main()
