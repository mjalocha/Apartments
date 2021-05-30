# Add path to scraping scripts
import sys
sys.path.append('Scraping')
sys.path.append('Database_scripts')
sys.path.append('Preprocessing scripts')

#Colab paths
sys.path.append('/content/Apartments')
sys.path.append('/content/Apartments/Scraping')
sys.path.append('/content/Apartments/Database_scripts')
sys.path.append('/content/Apartments/Preprocessing_scripts')
sys.path.append('/Apartments/Scraping')
sys.path.append('/Apartments/Database_scripts')
sys.path.append('/Apartments/Preprocessing_scripts')

from gratkaScraper import ScrapingGratka
from db_manipulation import DatabaseManipulation
from otodom import Preprocessing_Otodom
import pandas as pd
import configparser
import urllib
from sqlalchemy import create_engine

if __name__ == "__main__":
    # Database connection

    config = configparser.ConfigParser()
    config.read('/content/Apartments/Database_scripts/config.ini')

    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    # ===Gratka===
    gratka_scraper = ScrapingGratka(page='https://www.gratka.pl/nieruchomosci/mieszkania/', page_name='https://www.gratka.pl', max_threads=20)

    # Get links to scrape
    gratka_pages = gratka_scraper.get_pages()
    gratka_offers = gratka_scraper.get_offers(pages=gratka_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks = gratka_offers, page_name = "Gratka")

    #Push to scrape links to database
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)


    database_manipulation.push_to_scrape(to_scrape, "Gratka")

    # Scrape details
    gratka_scraped = gratka_scraper.get_details(offers=list(to_scrape["link"]),split_size=500)

    # Prepare offers to insert into table
    gratka_scraped_c = gratka_scraped.copy().reset_index().drop(['index'], axis=1)
    gratka_preprocess = Preprocessing_Otodom(apartment_details=gratka_scraped_c.where(pd.notnull(gratka_scraped_c), None),
                                             information_types=gratka_scraped_c.columns)
    gratka_table = gratka_preprocess.create_table()
    gratka_table=gratka_table.where(pd.notnull(gratka_table), None)

    # Insert details into table
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    database_manipulation.push_to_database_offers(offers=gratka_table, page_name = "Gratka")
