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

from otodomScraper import ScrapingOtodom
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

    # ===Otodom===
    otodom_scraper = ScrapingOtodom(page='https://www.otodom.pl/wynajem/mieszkanie/', page_name='https://www.otodom.pl', max_threads=20)

    # Get links to scrape
    otodom_pages = otodom_scraper.get_pages()
    otodom_offers = otodom_scraper.get_offers(pages=otodom_pages, split_size=100)
    to_scrape = database_manipulation.push_to_database_links(activeLinks = otodom_offers, page_name = "Otodom")

    #Push to scrape links to database
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)


    database_manipulation.push_to_scrape(to_scrape, "Otodom")

    # Scrape details
    otodom_scraped = otodom_scraper.get_details(offers=list(to_scrape["link"]),split_size=500)

    # Prepare offers to insert into table
    otodom_scraped_c = otodom_scraped.copy().reset_index().drop(['index'], axis=1)
    otodom_preprocess = Preprocessing_Otodom(apartment_details=otodom_scraped_c.where(pd.notnull(otodom_scraped_c), None),
                                             information_types=otodom_scraped_c.columns)
    otodom_table = otodom_preprocess.create_table()
    otodom_table=otodom_table.where(pd.notnull(otodom_table), None)

    # Insert details into table
    del database_manipulation
    database_manipulation = DatabaseManipulation(config = config, config_database = "DATABASE", table_name_links = "active_links",
                                                 table_name_offers = "preprocessing_offers", table_name_to_scrape = "to_scrape",
                                                 table_name_process_stage = "process_stage", split_size = 1000)

    database_manipulation.push_to_database_offers(offers=otodom_table, page_name = "Otodom")

