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

class ScrapingOtodom(Scraper):
    """
    A class used to scrape oferts from otodom.pl

    ...

    Attributes
    ----------
    page : str
        full main page name
    page_name : str
        specific page name which determines if you want to rent or buy/home or apartment etc.
    max_threads : int
        maximum number of threads (default 30)
    voivodeships : list
        list of voivodeships in Poland

    Methods
    -------
    scraping_pages_links(void: str) -> List[str]:
        Scraping pages based on voivodeships set to self.voivodeship variable in __init__

    get_pages() -> List[str]:
        The method called up by the user to download all links of the pages from otodom.pl

    scraping_offers_links(page_link: str) -> List[str]:
        Scraping offers links

    get_offers(pages: List = []) -> List[str]:
        The method called up by the user to download all links of the properties from otodom.pl

    get_details(split_size: int, offers: List = []) -> None:
        The method called up by the user to download all details about apartments.

    json_information_exception(obj: Dict[str, str], path: List[str], is_spatial: bool,
                is_address: bool = False, is_targetFeatures: bool = False, info_type: str = '') -> Union[List[str],str]:
        Verify weather there is possibility to extract specific information from json

    extract_target_features_information(obj: Dict[str, str], path: List[str]) -> str:
        Extract from json object target features information (eg. area, build-year)

    extract_localization_information(obj: Dict[str, str], path: List[str], is_address: bool, info_type: str) -> str:
        Extract from json object localization information (eg. address, region, district, city)

    extract_spatial_information(obj: Dict[str, str], path: List[str]) -> str:
        Extract from json object spatial information (eg. latitude, longitude)

    remove_styling(info_list: List[str]) -> List[str]:
        Remove styling substings

    extract_information_otodom(find_in: List[str], is_description: bool = False) -> Union[List[str], str]:
        Extract the information from the str (soup.find obj)

    scraping_offers_details(link: str) -> Union[DefaultDict[str,str], str]:
        Try to connect with offer link, if it is not possible save link to global list

    missed_details_func(links: List[str]) -> Tuple[List[str], List[str]]:
        Scrape missed details links
    """

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
        """Scraping pages based on voivodeships set to self.voivodeship variable in __init__

        Parameters
        ----------
        void: str
            voivodeship
        Returns
        ------
        list
            links to pages for voivodeship specified in void argument
        """
        # Create link
        link = self.page + void

        try:
            # Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)

            # Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_links_idClass(isId=False,
                                                                         to_find='pager',
                                                                         soup=soup_pages, replace=False)

            pages_range = self.prepare_range(pages_names)

            # Create all pages links
            all_pages_links = [link + '?page=' + str(page) for page in pages_range]

        except:
            all_pages_links = link

        return all_pages_links

    # The method called up by the user to download all links of the pages from otodom.pl
    def get_pages(self) -> List[str]:
        """The method called up by the user to download all links of the pages from otodom.pl

         Returns
         ------
         list
             list with pages for all voivodeships specified in __init__
         """

        # Scrape all links for voivodeships in self.voivodeship variable
        results_pages = self.scraping_all_links(self.scraping_pages_links, self.voivodeships)
        results_pages = self.flatten(results_pages)

        # Verify weather there are some missing oferts
        missed_pages = [oferts for oferts in results_pages if "page" not in oferts]

        if len(missed_pages) != 0:
            results_pages = self.flatten(
                [properties for properties in results_pages if (properties != None) & ("page" in properties)])

        # Try to scrape missing links once again and join them with scraped before
        missed_pages_list = self.missed_links_all(missed_offers=missed_pages, func=self.missed_offers_pages,
                                                  details=False, offers=False,
                                                  func_pages_or_offers=self.scraping_pages_links)
        results_pages = self.join_missed_with_scraped(missed_pages_list, results_pages)

        return self.flatten(results_pages)

    # Scraping offers links
    def scraping_offers_links(self, page_link: str) -> List[str]:
        """Scraping offers links

        Parameters
        ----------
        page_link: str
            link to specific page

        Returns
        ------
        list
            scraped offers links for specified in argument page link
        """

        try:
            # Read website, encode and create HTML parser
            soup_offers = self.enterPage_parser(page_link)

            properties_links = [art["data-url"] for art in soup_offers.select("article") if art.has_attr("data-url")]

            all_properties_links = properties_links

        except:
            all_properties_links = page_link

        return all_properties_links

    # Get districts and cities links
    def get_offers(self, split_size: int, pages: List = []) -> List[str]:
        """The method called up by the user to download all links of the properties from otodom.pl

        Parameters
        ----------
        split_size: int
           value divided by total number of links it is used to create splits to relieve RAM memory
        pages: list, optional
            for which pages the links to the properties are to be downloaded (default for all)

        Returns
        ------
        list
            flatten properties links
        """

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

        #Remove .html ending
        try:
            results_offers = results_offers.tolist()
            results_offers_all.append(results_offers)
        except:
            results_offers_all.append(results_offers)

        try:
            results_offers_all = [element.split(".html")[0] for element in results_offers_all]
        except:
            results_offers_all = [element.split(".html")[0] for element in np.concatenate(results_offers_all, axis=0)]

        return np.unique(results_offers_all).tolist()

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
            missed_details = [details for details in results_details if "www.otodom.pl" in details]
            results_details = self.flatten(
                [details for details in results_details if (details != None) & ("www.otodom.pl" not in details)])

            # Scrape missed links and join them with scraped before
            missed_details_list = self.missed_links_all(missed_offers=missed_details, func=self.missed_details_func,
                                                        restriction=5, details=True)
            results_details = self.join_missed_with_scraped(missed_details_list, results_details)

            # Information for user
            #print("%s splits left" % (len(splitted) - (splitted.index(split) + 1)))


            # Save scraped details as csv file
            results_details = [result for result in results_details if
                               (result != "Does not exist") & (result != None) & ("www.otodom.pl" not in result)]
            results.append(results_details)

        return pd.concat([pd.DataFrame(x) for x in results])


    # Verify weather there is possibility to extract specific information from json
    def json_information_exception(self, obj: Dict[str, str], path: List[str], is_spatial: bool,
                                   is_address: bool = False, is_targetFeatures: bool = False,
                                   info_type: str = '') -> Union[List[str], str]:

        """ Verify weather there is possibility to extract specific information from json

        Parameters
        ----------
        obj: Dict
            json object with details information
        path: str
            path to specific information in json object
        is_spatial: bool
            information weather variable that you are looking for is a spatial variable (latitude, longitude)
        is_address: bool, optional (default = False)
            information weather variable that you are looking for is an address variable
        is_targetFeatures: bool, optional (default = False)
            information weather variable that you are looking for is a target-feature variable (area, build-year)
        info_type: str, optional (default = '')
            information weather you are looking for region, district or city
        Returns
        ------
        list or str
            If they exist then the values searched for, and otherwise the value None
        """

        try:
            if is_spatial:
                return self.extract_spatial_information(obj,path)
            elif is_targetFeatures:
                return self.extract_target_features_information(obj, path)
            else:
                return self.extract_localization_information(obj, path, is_address, info_type)
        except:
            return None

    # Extract from jsob object target features information
    def extract_target_features_information(self, obj: Dict[str, str], path: List[str]) -> str:
        """ Extract from json object target features information (eg. area, build-year)

        Parameters
        ----------
        obj: Dict
            json object with details information
        path: str
            path to specific information in json object

        Returns
        ------
        str
            specified type of feature
        """

        return obj[path[0]][path[1]][path[2]][path[3]][path[4]]

    # Extract from jsob object localization information
    def extract_localization_information(self, obj: Dict[str, str], path: List[str], is_address: bool,
                                         info_type: str) -> str:
        """ Extract from json object localization information (eg. address, region, district, city)

        Parameters
        ----------
        obj: Dict
            json object with details information
        path: str
            path to specific information in json object
        is_address: bool
            information weather variable that you are looking for is an address variable
        info_type: str
            information weather you are looking for region, district or city

        Returns
        ------
        str
            information about address, region, district or city
        """
        temp_obj = obj[path[0]][path[1]][path[2]][path[3]][path[4]]

        if is_address:
            return temp_obj[0][path[5]]
        else:
            return [el['label'] for el in temp_obj if el['type'] == info_type][0]

    # Extract from jsob object spatial information
    def extract_spatial_information(self, obj: Dict[str, str], path: List[str]) -> str:
        """ Extract from json object spatial information (eg. latitude, longitude)

        Parameters
        ----------
        obj: Dict
            json object with details information
        path: str
            path to specific information in json object

        Returns
        ------
        str
            specified type of feature (eg. latitude, longitude)
        """

        return obj[path[0]][path[1]][path[2]][path[3]][path[4]][path[5]]

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
    def extract_information_otodom(self, find_in: List[str], is_description: bool = False) -> Union[List[str], str]:
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
                                                                         'css-46s0sq eu6swcv18']))

            subtitle = self.extract_information(self.soup_find_information(soup=soup_details,
                                                                 find_attr=['a', 'class',
                                                                            'css-1qz7z11 e1nbpvi61']))
            price = self.extract_information(self.soup_find_information(soup=soup_details,
                                                              find_attr=['strong', 'class',
                                                                         'css-srd1q3 eu6swcv17']))

            # Details and description (h2)
            details = self.extract_information_otodom(self.soup_find_information(soup=soup_details,
                                                                       find_attr=['div', 'class',
                                                                                  'css-1d9dws4 egzohkh2']))
            description = self.extract_information_otodom(soup_details.findAll("p").copy(), True)

            # Additional information (h3)
            additional_info_headers = [header.text for header in soup_details.findAll("h3")]
            additional_info = self.extract_information_otodom(
                soup_details("ul", attrs=["class", "css-13isnqa ex3yvbv0"]).copy(), True)

            # Information in json
            try:
                res = soup_details.findAll('script')
                lengths = [len(str(el)) for el in res]
                json_object = json.loads(res[lengths.index(max(lengths))].contents[0])

                # Longitude and Latitude
                lat = self.json_information_exception(obj=json_object,
                                                path=['props', 'pageProps', 'ad', 'location', 'coordinates', 'latitude'],
                                                is_spatial=True)
                lng = self.json_information_exception(obj=json_object,
                                                path=['props', 'pageProps', 'ad', 'location', 'coordinates', 'longitude'],
                                                is_spatial=True)

                # Adress and voivodeship
                address = self.json_information_exception(obj=json_object,
                                                    path=['props', 'pageProps', 'ad', 'location', 'address', 'value'],
                                                    is_spatial=False, is_address=True)
                voivodeship = self.json_information_exception(obj=json_object,
                                                        path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                        is_spatial=False, info_type="region")
                city = self.json_information_exception(obj=json_object,
                                                 path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                 is_spatial=False, info_type="city")
                district = self.json_information_exception(obj=json_object,
                                                 path=['props', 'pageProps', 'ad', 'location', 'geoLevels', 'label'],
                                                 is_spatial=False, info_type="district")

                # Target features (area, building floors num, etc.)
                features = ["Area", "Build_year", "Building_floors_num", "Building_material", "Building_type",
                            "Construction_status", "Deposit", "Floor_no", "Heating", "Rent", "Rooms_num"]
                values = []

                for feature in features:
                    offer_infos[feature] = self.json_information_exception(obj=json_object,
                                                                           path=['props', 'pageProps', 'ad', 'target',
                                                                                 feature],
                                                                           is_spatial=False, is_targetFeatures=True)


            except:
                features = ["Area", "Build-year", "Building_floors_num", "Building_material", "Building_type",
                            "Construction_status", "Deposit", "Floor_no", "Heating", "Rent", "Rooms_num"]
                lat = None
                lng = None
                price = np.NaN
                address = None
                voivodeship = None
                district = None
                for feature in features:
                    offer_infos[feature] = None

            # Assign information to dictionary
            offer_infos["city"] = city
            offer_infos["district"] = district
            offer_infos["address"] = address
            offer_infos["voivodeship"] = voivodeship
            offer_infos["title"] = title
            offer_infos["subtitle"] = subtitle
            offer_infos["price"] = price
            offer_infos["additional_info_headers"] = additional_info_headers
            offer_infos["additional_info"] = additional_info
            offer_infos["details"] = details
            offer_infos["description"] = description
            offer_infos["lat"] = lat
            offer_infos["lng"] = lng
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
