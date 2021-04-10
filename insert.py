import base64
import json
import os
import random
from random import seed, randint, uniform
import string
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import requests

class InsertDB(object):

    def __init__(self, hostname, user, password,dbname):
        self.host = hostname
        self.user = user
        self.password = password
        self.database = dbname



    def open_connection(self):

        con = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database = self.database
        )

        con.autocommit = True

        cur = con.cursor()

        return con, cur

    def show_databases(self):
        con, cur = self.open_connection()
        command = ("SELECT datname FROM pg_database;")

        cur.execute(command)

        dbs = cur.fetchall()

        con.close()

        return dbs

    def create_table_posters(self):
        tablename = "posters"
        con, cur = self.open_connection()

        self.drop_table(tablename)
        command = (
            """
            CREATE TABLE {}(
                poster_id SERIAL PRIMARY KEY,
                poster_content VARCHAR(255) NOT NULL,
                quantity INT NOT NULL,
                price DECIMAL NOT NULL,
                email VARCHAR(255) NOT NULL,
                sales_rep VARCHAR(255) NOT NULL,
                promo_code VARCHAR(255) NOT NULL);
                """.format(tablename)
        )

        cur.execute(command)

    def show_tables(self):
        con, cur = self.open_connection()
        command = (
            """SELECT table_name FROM information_schema.tables
            WHERE table_schema='public';
            """)
        cur.execute(command)

        tables = cur.fetchall()

        for table in tables:
            print(table,"\n")

        con.close()

        return tables

    def insert_into_source(self, tablename ,poster_content, quantity, price, email, sales_rep, promo_code):
        if self.database == "dw":
            return "wrong db"

        con, cur = self.open_connection()
        to_insert = (poster_content,quantity,price,email,sales_rep,promo_code)

        cur.execute(sql.SQL("INSERT INTO {} (poster_content,quantity, price,email,sales_rep,promo_code)\
            VALUES(%s,%s,%s,%s,%s,%s);".format(tablename)) , to_insert)

        con.close()

    def create_db(self,name):
        con, cur = self.open_connection()

        try:
            command = (""" 
            CREATE DATABASE {}
            """.format(name))
            cur.execute(command)
        except DuplicateDatabase:
            print("DB already exist")
        con.close()

    def show_table_data(self,tablename):
        con, cur = self.open_connection()

        command = (
            """SELECT  *  FROM {};""".format(tablename))

        cur.execute(command)

        table_data = cur.fetchall()

        return table_data

    def drop_table(self,tablename):
        con, cur = self.open_connection()

        command = ("""DROP TABLE IF EXISTS {};""".format(tablename))

        cur.execute(command)

        con.close()

    def filter_posters_table(self):
        if self.database == "dw":
            return "wrong db"

        __tablename__ = "posters"
        con, cur = self.open_connection()
        number_of_rows =len(self.show_table_data(__tablename__))
        
        command = (
            """ SELECT poster_content FROM {} 
            WHERE poster_id <= {} """.format(__tablename__,number_of_rows))

        cur.execute(command)
        poster_rows = cur.fetchall()

        names = []

        for t in poster_rows:
            names.append(t[0])

        return names

    def create_dw_films_table(self):
        if self.database == "source":
            return "wrong db"

        tablename = "films"
        self.drop_table(tablename)
        con, cur = self.open_connection()
        command = (
            """
            CREATE TABLE {}(
                film_id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                episode_id INT NOT NULL,
                director VARCHAR(255) NOT NULL,
                release_date DATE NOT NULL,
                poster_content VARCHAR(255) NOT NULL);
                """.format(tablename)
        )

        cur.execute(command)

        con.close()

    def insert_into_films(self, movie_title, episode_id, director_name, release_date, poster_content):
        if self.database == "source":
            return "wrong db"

        tablename = "films"

        con, cur = self.open_connection()
        to_insert = (movie_title, episode_id, director_name,release_date, poster_content)

        cur.execute(sql.SQL("INSERT INTO {} (title,episode_id, director, release_date ,poster_content)\
            VALUES(%s,%s,%s,%s,%s);".format(tablename)), to_insert)

        con.close()




        





seed(1)
hostname = "localhost"
user = "postgres"
password = "postgres"
database = "source"
db = InsertDB(hostname, user, password, database)


def random_char(y):
    return ''.join(random.choice(string.ascii_letters) for x in range(y))


def parse_starwars_names(json_data):
    names = []
    
    for k in json_data.keys():
        if k == "results":
            ships = k
            for i in json_data[ships]:
                for key in i.keys():
                    if key == "name":
                        names.append(i[key])

    return names


def secure_rand(len=8):
    token = os.urandom(len)
    return base64.b64encode(token)


if __name__ == "__main__":
    url = "http://swapi.dev/api/starships/"
    r = requests.get(url=url)
    api_data = json.dumps(r.json())
    data = json.loads(api_data)
    __tablename__ = "posters"

    ship_names = parse_starwars_names(data)

    db.create_table_posters()

    for name in ship_names[:]:
        poster_content = name
        quantity = randint(0, 50)
        price = round(uniform(1, 50), 2)
        email = random_char(7)+"@gmail.com"
        sales_rep = random_char(10)+"@gmail.com"
        promo_code = str(secure_rand())

        db.insert_into_source(__tablename__, poster_content, quantity,price, email, sales_rep, promo_code)
