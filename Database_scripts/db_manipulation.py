#Database manipulation

#libraries
import pyodbc
import sqlalchemy as sa
from sqlalchemy import create_engine
import urllib
import numpy as np
import pandas as pd
import configparser #configuration from ini file
from datetime import datetime

class DatabaseManipulation:
    """
    A class used to manage apartments database
    ...

    Methods
    -------
    connect_database(config, config_database, table_name):
        Connect to database
    create_split(dataFrame, split_size):
        Create splits to make it possible to insert or delete big datasets
    insert_active_links(dataFrame, column_names, split_size = 1000):
        Insert active links to database
    find_links_to_scrape(activeLinks, page_name, split_size = 1000):
        Find new links to scrape and inactive to remove
    replace_links(newLinks, removeLinks, page_name, split_size = 1000):
        Add new links to database and remove inactive
    push_to_database(activeLinks, page_name, split_size = 1000):
        Activate functions to replace and remove observations
    """

    def __init__(self, config, config_database, table_name_links, table_name_offers, table_name_to_scrape,
                 table_name_process_stage, split_size):
        """
        Parameters
        ----------
        config : ConfigParser
            params to connect with database
        config_database : str
            name of database in config file
        table_name_links : str
            name of table with active links
        table_name_offers : str
            name of table with offers
        table_name_to_scrape : str
            name of table for links to scrape
        table_name_process_stage : str
            name of table for process stage
        split_size : int
            size of splits
        """

        self.config = config
        self.config_database = config_database
        self.table_name_links = table_name_links
        self.table_name_offers = table_name_offers
        self.table_name_to_scrape = table_name_to_scrape
        self.table_name_process_stage = table_name_process_stage
        self.split_size = split_size
        self.engine = self.connect_database(config, config_database)

    #Connect to database
    def connect_database(self, config, database):
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

    #Create splits to make it possible to insert or delete big datasets
    def create_split(self, dataFrame):
        """Create splits to make it possible to insert or delete big datasets

        if dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations

        Returns
        ------
        list
            list of splits
        """

        #If dataFrame has has smaller number of rows than split_size value than assign as a split size dataFrame number of rows
        if(len(dataFrame) < self.split_size):
            splitted = [[0, len(dataFrame)]]
        else:
            splitted = np.array_split(list(range(0,len(dataFrame))), len(dataFrame)/self.split_size)
            splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]]  for elements in splitted]
            splitted[len(splitted) - 1][1] += 1

        return splitted

    #Insert active links to database
    def insert_active_links(self, dataFrame):
        """Insert active links to database

        if column names are correct insert observations to table

        Parameters
        ----------
        dataFrame : pd.DataFrame
            data frame with observations

        Returns
        ------
        str
            information that names are incorrect
        """

        # Add observations
        dataFrame.to_sql(self.table_name_links, schema='dbo', if_exists='append', con=self.engine, index=False)

    #Find new links to scrape and inactive to remove
    def find_links_to_scrape(self, activeLinks, page_name):
        """Find new links to scrape and inactive to remove

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped

        Returns
        ------
        list
            links that have to be scraped
        list
            links that are no longer available at webpage
        """

        #Select active links from database
        conn = self.engine.connect()
        links_database = pd.DataFrame.from_records(conn.execute("SELECT * FROM "+self.table_name_links+" WHERE [page_name] LIKE '"+page_name+"'").fetchall())

        #Find links to scrape and remove
        activeLinks = pd.DataFrame({"link": activeLinks})

        try:
            to_scrape = activeLinks[~activeLinks.stack().isin(links_database.iloc[:,2]).unstack()].dropna()
        except:
            to_scrape = activeLinks

        try:
            to_remove = links_database.iloc[:,2][~links_database.iloc[:,2].isin(activeLinks["link"])].dropna()
        except:
            to_remove = []

        conn.close()

        return to_scrape, to_remove

    #Add new links to database and remove inactive
    def replace_links(self, newLinks, removeLinks, page_name):
        """Find new links to scrape and inactive to remove

        Parameters
        ----------
        newLinks : list
            List of links to be inserted to the table
        removeLinks : list
            List of links to be removed from the table
        page_name : str
            name of the website from which data were scraped

        """

        #Delete links
        conn = self.engine.connect()

        if len(removeLinks) != 0:
            queries_delete = "DELETE FROM "+self.table_name_links+" WHERE [link] = '"+removeLinks+"'"

            for query in queries_delete:
                conn.execute(query)

        conn.close()

    def replace_offers(self, removeLinks):
        if len(removeLinks) > 0:
            #Change value for inavtive links
            conn = self.engine.connect()
            queries_active = "UPDATE " + self.table_name_offers + " SET [active] = 'No' WHERE [link] = '" + removeLinks + "'"
            queries_inactive_date =  "UPDATE " + self.table_name_offers + " SET [inactive_date] = '"+datetime.today().strftime('%Y-%m-%d')+"' WHERE [link] = '" + removeLinks + "'"

            for query in queries_active:
                conn.execute(query)

            for query in queries_inactive_date:
                conn.execute(query)

            conn.close()

    def insert_to_scrape_links(self, offers, page_name):

        engine = self.connect_database(self.config, self.config_database)

        # Push observations
        to_scrape = pd.DataFrame({"page_name": page_name, "link": offers["link"]})
        to_scrape.to_sql(self.table_name_to_scrape, schema='dbo', if_exists='append', con=engine, index=False)

        conn = engine.connect()
        query_pass = "UPDATE " + self.table_name_process_stage + " SET [scraping_offers] = 'T' WHERE [curr_date] in (SELECT TOP (1) [curr_date] FROM "+ self.table_name_process_stage +" ORDER BY [curr_date] DESC)"
        conn.execute(query_pass)

        conn.close()

    def add_process_stage(self, page_name):

        current_date = datetime.today().strftime('%Y-%m-%d')
        # Which process number
        conn = self.engine.connect()

        query = "SELECT * FROM " + self.table_name_process_stage + " WHERE page_name LIKE '"+ page_name +"' and curr_date LIKE  '" + current_date + "'"
        temp_table = conn.execute(query).fetchall()

        if len(temp_table) == 0:
            process_number = 1
        else:
            process_number = 2

        process_stage = pd.DataFrame(
            {"curr_date": [current_date], "process_number": process_number, "page_name": [page_name],
             "scraping_offers": ["F"], "scraping_details": ["F"]})
        process_stage.to_sql(self.table_name_process_stage, schema='dbo', if_exists='append', con=self.engine,
                             index=False)

        conn.close()

    def push_to_scrape(self, scrape, page_name):

        # Update process_table
        self.insert_to_scrape_links(offers=scrape, page_name=page_name)

    #Activate functions to replace and remove observations
    def push_to_database_links(self, activeLinks, page_name):
        """Activate functions to replace and remove observations

        Parameters
        ----------
        activeLinks : list
            list of scraped links (offers are available at webpage)
        page_name : str
            name of the website from which data were scraped

        """

        #Create process stage observation
        self.add_process_stage(page_name = page_name)

        #Find which links has to be scraped and which to removed
        scrape, remove = self.find_links_to_scrape(activeLinks = activeLinks, page_name = page_name)

        #Delete and insert links
        self.replace_links(newLinks = list(set(scrape)), removeLinks = remove, page_name = page_name)

        #Update table with offers
        self.replace_offers(removeLinks = remove)

        return scrape

    def push_to_database_offers(self, offers, page_name):

        # Push observations
        offers.to_sql(self.table_name_offers, schema='dbo', if_exists='append', con=self.engine, index=False)

        conn = self.engine.connect()
        query_pass = "UPDATE " + self.table_name_process_stage + " SET [scraping_details] = 'T' WHERE [curr_date] in (SELECT TOP (1) [curr_date] FROM "+ self.table_name_process_stage +" ORDER BY [curr_date] DESC)"
        conn.execute(query_pass)

        conn.close()

        #Insert links
        newLinks = pd.DataFrame({"page_name": page_name, "link": offers["link"]})
        self.insert_active_links(dataFrame = newLinks)

