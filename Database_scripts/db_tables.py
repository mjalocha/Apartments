#Tables

#libraries
import pyodbc
import numpy as np
import pandas as pd
import configparser
from sqlalchemy import create_engine
import urllib

def create_table(engine, query):
    conn = engine.connect()
    conn.execute(query)
    conn.close()

def connect_database(config, database):
    """Connect to database
    Parameters
    ----------
    config : ConfigParser
        params to connect with database
    database : str
        database name in config file
    Returns
    ------
    sqlalchemy engine
        connection with database
    """

    params = urllib.parse.quote_plus(
        'Driver=%s;' % config.get(database, 'DRIVER') +
        'Server=%s,1433;' % config.get(database, 'SERVER') +
        'Database=%s;' % config.get(database, 'DATABASE') +
        'Uid=%s;' % config.get(database, 'USERNAME') +
        'Pwd={%s};' % config.get(database, 'PASSWORD') +
        'Encrypt=yes;' +
        'TrustServerCertificate=no;' +
        'Connection Timeout=30;')

    conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
    engine = create_engine(conn_str)

    return engine

#Database connection
config = configparser.ConfigParser() 
config.read('Database_scripts/config.ini')
engine = connect_database(config, "DATABASE")
conn = engine.connect()
query = "UPDATE " + "process_stage" + " SET [scraping_details] = 'T' WHERE [curr_date] in (SELECT TOP (1) [curr_date] FROM "+ "process_stage" +" ORDER BY [curr_date] DESC)"
conn.execute(query)

#Create tables

create_activeLinks = "CREATE TABLE active_links (link_id INT IDENTITY PRIMARY KEY,\
                                           page_name NVARCHAR (256) NOT NULL,\
                                           link NVARCHAR(512) NOT NULL)"

create_preprocessing_offers = "CREATE TABLE preprocessing_offers (offer_id INT IDENTITY PRIMARY KEY," \
"area FLOAT," \
"latitude FLOAT," \
"longitude FLOAT," \
"link NVARCHAR (512)," \
"price FLOAT," \
"currency NVARCHAR(256)," \
"rooms NVARCHAR (256)," \
"floors_number NVARCHAR (256)," \
"floor NVARCHAR (256)," \
"type_building NVARCHAR (256)," \
"material_building NVARCHAR (256)," \
"year NVARCHAR (256)," \
"headers NVARCHAR (4000)," \
"additional_info NVARCHAR (4000)," \
"city NVARCHAR (256)," \
"address NVARCHAR (256)," \
"district NVARCHAR (256)," \
"voivodeship NVARCHAR (256)," \
"active NVARCHAR (256)," \
"scrape_date NVARCHAR (256)," \
"inactive_date NVARCHAR (256)," \
"page_name NVARCHAR (256), " \
"offer_title NVARCHAR (256), " \
"description_1 NVARCHAR (4000)," \
"description_2 NVARCHAR (4000)," \
"description_3 NVARCHAR (4000)," \
"description_4 NVARCHAR (4000))"

create_to_scrape = "CREATE TABLE to_scrape (link_id INT IDENTITY PRIMARY KEY," \
"page_name NVARCHAR (256) NOT NULL," \
"link NVARCHAR (512) NOT NULL)"

create_missing_links = "CREATE TABLE missing_links (missing_id INT IDENTITY PRIMARY KEY," \
"page_name NVARCHAR (256) NOT NULL," \
"link NVARCHAR (512) NOT NULL," \
"link_type NVARCHAR (256) NOT NULL)"

create_process_stage = "CREATE TABLE process_stage (process_id INT IDENTITY PRIMARY KEY," \
"curr_date NVARCHAR (256) NOT NULL," \
"process_number INT NOT NULL," \
"page_name NVARCHAR (256) NOT NULL," \
"scraping_offers NVARCHAR (256) NOT NULL," \
"scraping_details NVARCHAR (256) NOT NULL)"

create_table(engine, create_activeLinks)
create_table(engine, create_preprocessing_offers)
create_table(engine, create_to_scrape)
create_table(engine, create_missing_links)
create_table(engine, create_process_stage)

#Database connection
config = configparser.ConfigParser()
config.read('Database_scripts/config.ini')

engine = connect_database(config, "DATABASE")

conn = engine.connect()
conn.execute("DROP TABLE preprocessing_offers")
conn.execute("DROP TABLE active_links")
conn.execute("DROP TABLE to_scrape")
conn.execute("DROP TABLE missing_links")
conn.execute("DROP TABLE process_stage")

engine = connect_database(config, "DATABASE")
conn = engine.connect()

query = "SELECT * FROM preprocessing_offers"
to_scrape = pd.read_sql(query, conn)

query = "SELECT * FROM process_stage"
process_stage = pd.read_sql(query, conn)

query = "SELECT * FROM preprocessing_offers"
preprocessing_offers = pd.read_sql(query, conn)
preprocessing_offers.to_csv("preprocessing_offers.csv")

query = "SELECT * FROM active_links"
active_links = pd.read_sql(query, conn)
active_links.to_csv("active_links.csv")

query = "SELECT * FROM to_scrape"
to_scrape = pd.read_sql(query, conn)
to_scrape.to_csv("to_scrape.csv")

query = "SELECT * FROM process_stage"
process_stage = pd.read_sql(query, conn)
process_stage.to_csv("process_stage.csv")