# Scraping Otodom

# Add path to scraper.py
import sys
sys.path.append('Scraping')

# Libraries
from bs4 import BeautifulSoup
from collections import defaultdict
import numpy as np
import pandas as pd
from scraper import Scraper
from datetime import datetime
import json
from typing import Tuple, List, DefaultDict, Union, Dict
import re
import itertools

class ScrapingGratka(Scraper):

    def __init__(self, page, page_name, max_threads=30):
        """
        Parameters
        ----------
        page : str
            full main page name
        page_name : str
            specific page name which determines if you want to rent or buy/home or apartment etc.
        max_threads : int
            maximum number of threads (default 30)
        """

        self.page = page
        self.max_threads = max_threads
        self.page_name = page_name
        self.voivodeships = ["dolnoslaskie", "kujawsko-pomorskie","lodzkie","lubelskie","lubuskie","malopolskie","mazowieckie",
                             "opolskie", "podkarpackie", "podlaskie", "pomorskie", "slaskie", "warminsko-mazurskie",
                             "wielkopolskie","zachodniopomorskie"]

    # Scraping pages links
    def scraping_pages_links(self, void: str) -> List[str]:

        # Create link
        link = self.page + void + "/wynajem"
        try:
            # Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)

            # Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_links_idClass(isId=False,
                                                                         to_find='pagination',
                                                                         soup=soup_pages, replace=False)

            pages_range = self.prepare_range(pages_names)

            # Create all pages links
            all_pages_links = [link + '?page=' + str(page) for page in pages_range]

        except:
            all_pages_links = link

        return all_pages_links

    # The method called up by the user to download all links of the pages from gratka.pl
    def get_pages(self) -> List[str]:

        # Scrape all links for voivodeships in self.voivodeship variable
        results_pages = self.scraping_all_links(self.scraping_pages_links, self.voivodeships)
        results_pages = self.flatten(results_pages)

        # Verify weather there are some missing oferts
        #missed_pages = [oferts for oferts in results_pages if "page" not in oferts]

        #if len(missed_pages) != 0:
        #    results_pages = self.flatten(
        #        [properties for properties in results_pages if (properties != None) & ("page" in properties)])

        # Try to scrape missing links once again and join them with scraped before
        #missed_pages_list = self.missed_links_all(missed_offers=missed_pages, func=self.missed_offers_pages,
        #                                          details=False, offers=False,
        #                                          func_pages_or_offers=self.scraping_pages_links)
        #results_pages = self.join_missed_with_scraped(missed_pages_list, results_pages)

        return self.flatten(results_pages)

    # Scraping offers links
    def scraping_offers_links(self, page_link: str) -> List[str]:

        try:
            # Read website, encode and create HTML parser
            soup_offers = self.enterPage_parser(page_link)

            properties_links = [art["data-href"] for art in soup_offers.select("article") if art.has_attr("data-href")]

            all_properties_links = properties_links

        except:
            all_properties_links = page_link

        return all_properties_links

    # Get districts and cities links
    def get_offers(self, split_size: int, pages: List = []) -> List[str]:


        # Verify whether user want to specify specific pages
        if any(pages):
            results_pages = pages
        else:
            results_pages = self.get_pages()

        # Create splits to relieve RAM memory# Create splits to relieve RAM memory
        splitted = self.create_split(links = results_pages, split_size = split_size)
        results_offers_all = list()

        for split in splitted:
            # Scrape all offers
            results_offers = self.scraping_all_links(self.scraping_offers_links, results_pages[split[0]:split[1]])

            #Verify weather there are some missing offers
            missed_offers = [offers for offers in results_offers if "page" in offers]
            results_offers = np.concatenate(
                [properties for properties in results_offers if (properties != None) & ("page" not in properties)], axis=0)

            # Scrape missing offers and join them with scraped before
            missed_offers_list = self.missed_links_all(missed_offers=missed_offers, func=self.missed_offers_pages,
                                                       details=False, offers=True,
                                                       func_pages_or_offers=self.scraping_offers_links)

            results_offers = self.join_missed_with_scraped(missed_offers_list, results_offers)

            results_offers_all.append(results_offers)

        results_offers_all = self.flatten(results_offers_all)
        return results_offers_all

    # Get apartments details
    def get_details(self, split_size: int, offers: List = []) -> None:
        """The method called up by the user to download all details about apartments. Results are saved to
        number_of_links/split.csv files

        Parameters
        ----------
        split_size: int
           value divided by total number of links it is used to create splits to relieve RAM memory
        offers: list, optional
            for which offers links the properties details are to be scraped (default for all)

        """

        # Verify whether user want to specify specific pages
        if any(offers):
            results_offers = offers
        else:
            results_offers = self.get_offers()

        # Create splits to relieve RAM memory
        splitted = self.create_split(links=results_offers, split_size=split_size)
        results = list()

        # Scrape details
        for split in splitted:
            results_details = self.scraping_all_links(self.scraping_offers_details_exceptions,
                                                      results_offers[split[0]:split[1]])
            # Assign to variables missed links and scraped properly
            missed_details = [details for details in results_details if "www.gratka.pl" in details]
            results_details = self.flatten(
                [details for details in results_details if (details != None) & ("www.gratka.pl" not in details)])

            # Scrape missed links and join them with scraped before
            missed_details_list = self.missed_links_all(missed_offers=missed_details, func=self.missed_details_func,
                                                        restriction=5, details=True)
            results_details = self.join_missed_with_scraped(missed_details_list, results_details)

            # Information for user
            #print("%s splits left" % (len(splitted) - (splitted.index(split) + 1)))


            # Save scraped details as csv file
            results_details = [result for result in results_details if
                               (result != "Does not exist") & (result != None) & ("www.gratka.pl" not in result)]
            results.append(results_details)

        return pd.concat([pd.DataFrame(x) for x in results])

    # Remove styling substings
    def remove_styling(self, info_list: List[str]) -> List[str]:
        """ Remove styling substings (eg. .css.*?}, @media.*?})

        Parameters
        ----------
        info_list: list
            information from offer (eg. details, additional info)

        Returns
        ------
        list
            list without styling substings
        """

        info_list = [re.sub('.css.*?}', '', element) for element in info_list]
        info_list = [re.sub('@media.*?}', '', element) for element in info_list]

        return info_list

    #Extract the information from the str (soup.find obj).
    def extract_information_gratka(self, find_in: List[str], is_description: bool = False) -> Union[List[str], str]:
        """Extract the information from the str (soup.find obj).
         If it is a description replace the html tags with newline characters, otherwise remove the empty strings from the list.

        Parameters
        ----------
        find_in: list
            object where used to find information
        is_description: boolean, (default = False)
            determines whether it is a description (in description replace html tags with new line tags)

        Returns
        ------
        list or str
            1. elements with specific attributes
            2. "None" informs that information is not available
        """

        try:
            if is_description:
                # Replace html tags with new line tags
                [elem.replace_with(elem.text + "\n\n") for element in find_in for elem in
                 element.find_all(["a", "p", "div", "h3", "br", "li"])]
                temp = [element.text for element in find_in]

                return self.remove_styling(temp)
            else:
                temp = [element.text for element in find_in if element.text != '']

                return self.remove_styling(temp)
        except:
            return None

    def extract_localization_information(self, list_of_scripts, temp = 'locationParams'):
        for script in list_of_scripts:
            script_string = str(script)
            if temp in script_string:
                return re.search('%s(.*)' % ('locationParams'), script_string).group(1)

        return None

    # Scraping details from offer
    def scraping_offers_details(self, link: str) -> Union[DefaultDict[str,str], str]:
        """Try to connect with offer link, if it is not possible save link to global list.
         Also try to scrape information from json object

        Parameters
        ----------
        link: str
           link to offer

        Returns
        ------
        defaultdict or str
            1. the details of the flat
            2. information that offer is no longer available
        """
        # Scraping details from link
        offer_infos = defaultdict(list)
        soup_details = self.enterPage_parser(link)

        try:
            # Title and subtitle
            title = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['h1', 'class',
                                                                         'sticker__title']))

            price = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['span', 'class',
                                                                         'priceInfo__value']))[0]
            price_currency = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['span', 'class',
                                                                         'priceInfo__value']))[1]

            # Details
            details = self.extract_information_gratka(
                soup_details("ul", attrs=["class", "parameters__rolled"]).copy(), True)

            # Description
            description = self.extract_information(self.soup_find_information(soup=soup_details,
                                                                    find_attr=['div', 'class',
                                                                               'description__rolled ql-container']))

            # Information in json
            try:
                res = soup_details.findAll('script')
                location_params = self.extract_localization_information(list_of_scripts=res)
            except:
                location_params = None

            # Assign information to dictionary
            offer_infos["location_params"] = location_params
            offer_infos["title"] = title
            offer_infos["price"] = price
            offer_infos["price_currency"] = price_currency
            offer_infos["details"] = details
            offer_infos["description"] = description
            offer_infos["link"] = link

            return offer_infos

        except:
            return "Does not exist"

    # Scrape missed details links
    def missed_details_func(self, links: List[str]) -> Tuple[List[str], List[str]]:
        """Scrape missed details links

        Parameters
        ----------
        links: list
            missing links

        Returns
        ------
        list, list
            1. scraped missed links
            2. links that are still missing
        """

        links = self.scraping_all_links(self.scraping_offers_details_exceptions, links)

        # Assign missed links to variable
        missed_links = [details for details in links if "www.otodom.pl" in details]

        return links, missed_links
