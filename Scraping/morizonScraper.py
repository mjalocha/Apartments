#Scraping Morizon
import sys
sys.path.append('Scraping')

#Libraries
from bs4 import BeautifulSoup
from urllib.request import urlopen
from collections import defaultdict
from datetime import datetime
import concurrent.futures
from scraper import Scraper
import numpy as np
import pandas as pd
from typing import Tuple, List, Callable, DefaultDict, Union, Dict

MAX_THREADS = 30
PAGE_NAME = 'https://www.morizon.pl'


class ScrapingMorizon(Scraper):
    """
    A class used to scrape offers from morizon.pl

    ...

    Attributes
    ----------
    page : str
        full main page name
    page_name : str
        specific page name which determines if you want to rent or buy/home or apartment etc.
    max_threads : int
        maximum number of threads (default 30)

    Methods
    -------
    scraping_cities_and_districts_links(page: str) -> Tuple[List[str], List[str]]
        Scraping cities and running function to scrape districts

    scraping_districts_links(city_link: str) -> List[str]:
        Scraping cities and districts

    get_districts_cities() -> List[str]:
        Get districts links

    scraping_pages_links(district_link: str) -> List[str]:
        Scraping pages links

    get_pages(districts: List[str] = []) -> List[str]:
        The method called up by the user to download all links of the pages from morizon.pl

    scraping_offers_links(page_link: str) -> List[str]:
        Scraping offers links

    get_offers(pages: List[str] = []) -> List[str]:
        Get districts and cities links

    missed_details_func(links: List[str]) -> Tuple[List[str], List[str]]:
        Scrape missed details links

    get_details(split_size: int, offers: List[str] = []) -> None:
        The method called up by the user to download all details about apartments.

    scraping_offers_details(link: str) -> Union[DefaultDict[str,str], str]:
        Scraping details from offer

    information_exists(details: List[str]) -> Union[List[str], str]:
        Verify if basic information in 'em' tag exists

    spatial_data_exists(data: List[str], kind: str) -> Union[List[str], str]:
        Verify if information about apartment location exists
    """

    def __init__(self, page, page_name, max_threads = 30):
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
        
    #Scraping cities and running function to scrape districts
    def scraping_cities_and_districts_links(self, page: str) -> Tuple[List[str], List[str]]:
        """Scraping cities and running function to scrape districts

        Parameters
        ----------
        page: str
            full main page name 
        Returns
        ------
        list, list
            1. links to cities
            2. links to individual city districts
        """
        
        #Read website, encode and create HTML parser
        soup_cities = self.enterPage_parser(page)

        #Extract cities names and links
        cities_names, cities_newest_links = self.extract_links_idClass(isId = True, to_find = 'locationListChildren',soup = soup_cities, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])

        threads = min(self.max_threads, len(cities_newest_links))

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(self.scraping_districts_links, cities_newest_links))
            
        return cities_newest_links, results
    
    #Scraping districts links
    def scraping_districts_links(self, city_link: str) -> List[str]:
        """Scraping districts links

        Parameters
        ----------
        city_link: str
            link to specific city
        Returns
        ------
        list
            links to individual city districts
        """
        #Create city link
        link = self.page_name + city_link

        try:
            #Read website, encode and create HTML parser
            soup_districts = self.enterPage_parser(link)
            
            #Extract districts names and links
            districts_names, districs_links = self.extract_links_idClass(isId = True, to_find = 'locationListChildren', soup = soup_districts, replace = True, replace_to = ["mieszkania", "mieszkania/najnowsze"])
        
            districs_newest_links = districs_links
        
        except:
            districs_newest_links = link
            
        return districs_newest_links
            
    #Get districts links
    def get_districts_cities(self) -> List[str]:
        """Scraping districts links

        Returns
        ------
        list
            links to individual city districts
        """
        
        cities_newest_links, results_districts = self.scraping_cities_and_districts_links(self.page)
        results_districts = np.concatenate([districts for districts in results_districts if districts is not None], axis=0)
        
        return results_districts

    #Scraping pages links
    def scraping_pages_links(self, district_link: str) -> List[str]:
        """Scraping pages links

        Parameters
        ----------
        district_link: str
            link to specific district

        Returns
        ------
        list
            scraped pages links
        """
        
        #Create link
        link = self.page_name + district_link
        
        try:
            #Read website, encode and create HTML parser
            soup_pages = self.enterPage_parser(link)
             
            #Extract pages numbers and links
            pages_names, pages_newest_links = self.extract_links_idClass(isId = False, to_find = 'nav nav-pills mz-pagination-number', soup = soup_pages,replace = False)

            pages_range = self.prepare_range(pages_names)
            
            #Create all pages links
            pages_links = [link + '?page=' + str(page) for page in pages_range]
            
            all_pages_links = pages_links
        
        except:
            all_pages_links = link
            
        return all_pages_links
    
    #The method called up by the user to download all links of the pages from morizon.pl
    def get_pages(self, districts: List[str] = []) -> List[str]:
        """The method called up by the user to download all links of the pages from morizon.pl

        Parameters
        ----------
        districts: list, optional
            for which districts the links to the pages are to be downloaded (default for all)

        Returns
        ------
        list
            flatten pages links
        """
        
        #Verify whether user want to specify specific districts
        if any(districts):
            results_districts = districts
        else:
            results_districts = self.get_districts_cities()
        
        results_pages = self.scraping_all_links(self.scraping_pages_links,results_districts)
        results_pages = self.flatten(results_pages)
        
        missed_pages = [offers for offers in results_pages if "page" not in offers]
        
        if len(missed_pages) != 0:
            results_pages = self.flatten([properties for properties in results_pages if (properties != None) & ("page" in properties)])

        missed_pages_list = self.missed_links_all(missed_offers = missed_pages, func = self.missed_offers_pages, details = False, offers = False, func_pages_or_offers = self.scraping_pages_links)
        results_pages = self.join_missed_with_scraped(missed_pages_list,results_pages)

        return self.flatten(results_pages)
    
    #Scraping offers links
    def scraping_offers_links(self, page_link: str) -> List[str]:
        """Scraping offers links

        Parameters
        ----------
        page_link: str
            link to specific page

        Returns
        ------
        list
            scraped offers links
        """
        
        try:
            #Read website, encode and create HTML parser
            soup_offers = self.enterPage_parser(page_link)
               
            properties_links = soup_offers.findAll(class_ = "property_link")
            properties_links = [link.get("href") for link in properties_links if("oferta" in link.get("href"))]
            
            all_properties_links = properties_links
    
        except:
            all_properties_links = page_link
            
        return all_properties_links
    
    #Get districts and cities links
    def get_offers(self, split_size: int, pages: List[str] = []) -> List[str]:
        """The method called up by the user to download all links of the properties from morizon.pl

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
        
        #Verify whether user want to specify specific pages
        if any(pages):
            results_pages = pages
        else:
            results_pages = self.get_pages()

        #Create splits to relieve RAM memory
        splitted = self.create_split(links=results_pages, split_size=split_size)

        results_offers_all = list()
        for split in splitted:

            # Scrape all offers
            results_offers = self.scraping_all_links(self.scraping_offers_links,results_pages[split[0]:split[1]])

            # Verify weather there are some missing offers
            missed_offers = [offers for offers in results_offers if "page" in offers]
            results_offers = [properties for properties in self.flatten(results_offers) if ("page" not in properties)]

            # Scrape missing offers and join them with scraped before
            missed_offers_list = self.missed_links_all(missed_offers = missed_offers, func = self.missed_offers_pages, details = False, offers = True, func_pages_or_offers = self.scraping_offers_links)
            results_offers = self.join_missed_with_scraped(missed_offers_list,results_offers)

            results_offers_all.append(results_offers)

        return self.flatten(results_offers_all)
    
    #Scrape missed details links
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
        
        links = self.scraping_all_links(self.scraping_offers_details_exceptions,links)
        
        #Assign missed links to variable
        missed_links = [details for details in links if "www.morizon.pl" in details]
        
        return links, missed_links

    #Get apartments details
    def get_details(self, split_size: int, offers: List[str] = []) -> None:
        """The method called up by the user to download all details about apartments.
         Results are saved to number_of_links/split.csv files

        Parameters
        ----------
        split_size: int
           value divided by total number of links it is used to create splits to relieve RAM memory
        offers: list, optional
            for which offers links the properties are to be downloaded (default for all)

        """

        #Verify whether user want to specify specific pages
        if any(offers):
            results_offers = offers
        else:
            results_offers = self.get_offers()

        #Create splits to relieve RAM memory
        splitted = self.create_split(links=results_offers, split_size=split_size)
        results_details_all = list()

        for split in splitted:

            results_details = self.scraping_all_links(self.scraping_offers_details_exceptions,results_offers[split[0]:split[1]])

            #Assign to variables missed links and scraped properly
            missed_details = [details for details in results_details if "www.morizon.pl" in details]
            results_details = self.flatten([details for details in results_details if (details != None) & ("www.morizon.pl" not in details)])
            
            #Scrape missed links and join them to already scraped
            missed_details_list = self.missed_links_all(missed_offers = missed_details, func = self.missed_details_func, restriction = 5, details = True)
            results_details = self.join_missed_with_scraped(missed_details_list,results_details)
            
            #Information for user
            #print("%s splits left" %(len(splitted) - (splitted.index(split) + 1)))

            #Save scraped details as csv file
            results_details = [result for result in results_details if (result != "Does not exist") & (result != None) & ("www.morizon.pl" not in result)]

            results_details_all.append(results_details)

        return pd.concat([pd.DataFrame(x) for x in results_details_all])
    
    #Scraping details from offer
    def scraping_offers_details(self, link: str) -> Union[DefaultDict[str,str], str]:
        """Try to connect with offer link, if it is not possible save link to global list

        Parameters
        ----------
        link: str
           offer link
         
        Returns
        ------
        defaultdict or str
            1. the details of the flat
            2. information that offer is no longer available
        """
        
        #Scraping details from link
        offer_infos = defaultdict(list)
        soup_details = self.enterPage_parser(link)
        try:
            #Title and subtitle
            title = self.extract_information(self.soup_find_information(soup = soup_details,
                                                                   find_attr = ['div', 'class', 'summaryLocation clearfix row']).find_all("span"))
               
            subtitle = self.extract_information(self.soup_find_information(soup = soup_details,
                                                                    find_attr = ['div', 'class', 'summaryTypeTransaction clearfix']))

            # Localization path
            localization_path = self.extract_information(self.soup_find_information(soup=soup_details,
                                                                          find_attr=['nav', 'class',
                                                                                     'breadcrumbs']).find_all("span"))
            #Basic information
            price = self.information_exists(self.soup_find_information(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconPrice']))
            priceM2 = self.information_exists(self.soup_find_information(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconPriceM2']))
            area = self.information_exists(self.soup_find_information(soup = soup_details,
                                                                 find_attr = ['li', 'class', 'paramIconLivingArea']))
            rooms = self.information_exists(self.soup_find_information(soup = soup_details,
                                                                    find_attr = ['li', 'class', 'paramIconNumberOfRooms']))
               
               
            #Params
            params = soup_details.find(class_ = "propertyParams")
            params_h3 = [element.text for element in params.findAll("h3")]
            params_tables = [element.text for element in params.findAll("table")]
            params_p = [element.text for element in params.findAll("p")]
            
            #Description
            description = self.extract_information(self.soup_find_information(soup = soup_details,
                                                                find_attr = ['div', 'class', 'description']),True, "p")
            soup_details.find("section", attrs={"class": "propertyMap"})
            
            #Longitude and Latitude
            google_map = self.soup_find_information(soup = soup_details, find_attr = ['div', 'class', 'GoogleMap'])
            lat = self.spatial_data_exists(google_map, 'data-lat')
            lng = self.spatial_data_exists(google_map, 'data-lng')
            
            #Assign information to dictionary
            #offer_infos["city"] = city_name
            #offer_infos["district"] = district.split("najnowsze/")[1].split("/")[1]
            offer_infos["title"] = title
            offer_infos["subtitle"] = subtitle
            offer_infos["localization_path"] = localization_path
            offer_infos["price"] = price
            offer_infos["priceM2"] = priceM2
            offer_infos["area"] = area
            offer_infos["rooms"] = rooms
            offer_infos["params_h3"] = params_h3
            offer_infos["params_tables"] = params_tables
            offer_infos["params_p"] = params_p
            offer_infos["description"] = description
            offer_infos["lat"] = lat
            offer_infos["lng"] = lng
            offer_infos["link"] = link
            
            return(offer_infos)
            
        except:
            return "Does not exist"
    
    #Verify if basic information in 'em' tag exists
    def information_exists(self, details: List[str]) -> Union[List[str], str]:
        """Verify if basic information in 'em' tag exists

        Parameters
        ----------
        details: list
            elements with specific attributes
            
        Returns
        ------
        list or str
            1. elements with specific attributes
            2. "None" informs that information is not available
        """
        
        try:
            return self.extract_information(details.em)
        except:
            return None
    
    #Verify if information about apartment location exists
    def spatial_data_exists(self, data: List[str], kind: str) -> Union[List[str], str]:
        """Verify if information about apartment location exists

        Parameters
        ----------
        data: list
            extracted information about longitude and latitude
        kind: str
            determines whether longitude or latitude is searched

        Returns
        ------
        list or str
            1. elements with specific attributes
            2. "None" informs that information is not available

        """

        try:
            return data[kind]
        except:
            return None
