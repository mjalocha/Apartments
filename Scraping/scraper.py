# Libraries
from bs4 import BeautifulSoup
from urllib.request import urlopen
import concurrent.futures
import numpy as np
from typing import Tuple, List, Callable, DefaultDict, Union
import pandas as pd

class Scraper:
    """
    General class from which classes to scrape specific offer pages inherit

    Methods
    -------
    enterPage_parser(link: str) -> BeautifulSoup:
        Read website, encode and create HTML parser

    extract_links_idClass(isId: bool, to_find: str, soup: BeautifulSoup, replace: bool,
                        replace_to: List[str] = []) -> Tuple[List[str], List[str]]:
        Extract links with id or class tag

    prepare_range(pages_names: List[str]) -> range:
        Prepare pages range

    create_split(links: List[str], split_size: int) -> List[range]:
        Create splits to relieve RAM memory

    flatten(result_to_flatt: List[List[str]]) -> Union[List[List[str]],List[str]]:
        Flatten a list

    scraping_all_links(func: Callable, all_links: List[str]) -> List[DefaultDict[str, str]]:
        General function to scrape links that activates ThreadPoolExecutor

    missed_offers_pages(links: List[str], offers: bool, func: Callable) -> Tuple[List[DefaultDict[str, str]],List[str]]:
        Scrape missed offers and pages links

    missed_links_all(missed_offers: List[str], func: Callable, details: bool, restriction: int = 5, offers: bool = None,
                    func_pages_or_offers: Callable = None) -> List:
        Scrape omitted data until you have scraped all

    join_missed_with_scraped(missed: List[str], scraped: List[str]) -> List:
        Join missed information with already scraped

    scraping_offers_details_exceptions(link: str) -> Union[DefaultDict[str, str], str]:
        Try to connect with offer link, if it is not possible save link to global list

    soup_find_information(soup: BeautifulSoup, find_attr: List[str]) -> List[str]:
        Find in soup with 3 args

    extract_information(find_in: BeautifulSoup, find_with_obj: bool = False, obj: str = None) -> Union[List[str], str]:
        Extract strings from infos founded in soup
    """

    # Read website, encode and create HTML parser
    def enterPage_parser(self, link: str) -> BeautifulSoup:
        """Read website, encode and create HTML parser

        try to encode with "utf-8" if it creates error then use "laitn-1"

        Parameters
        ----------
        link : str
            link to web page which you want to parse

        Returns
        ------
        BeautifulSoup
            a beautifulsoup object used to extract useful information
        """

        # Get website
        URL = link
        page = urlopen(URL)

        # Read website, encode and create HTML parser
        html_bytes = page.read()
        try:
            html = html_bytes.decode("utf-8")
        except:
            html = html_bytes.decode("latin-1")

        return BeautifulSoup(html, "html.parser")

    # Extract links with id or class tag
    def extract_links_idClass(self, isId: bool, to_find: str, soup: BeautifulSoup, replace: bool,
                              replace_to: List[str] = []) -> Tuple[List[str], List[str]]:
        """Extract links with id or class tag

        extracting links with id or class tag

        Parameters
        ----------
        isId: boolean
            determines whether to look for an id or a class
        to_find: str
            name of class or id
        soup: BeautifulSoup
            object used to extract information
        replace: boolean
            determines whether part of the link is to be replaced
        replace_to: list, optional
            two elements list containing what [0] has to be replaces with what [1]

        Returns
        ------
        list, list
            1. list containing names of extracted links e.g.  districts, cities.
            2. list containing extrated links e.g. districts, pages
        """

        # Find by id or class
        if (isId):
            extracted = soup.find(id=to_find)
        else:
            extracted = soup.find(class_=to_find)

        # If there is only one page assign empty arrays to variables
        try:
            # Find all a tag's
            extracted_names = [name.string for name in extracted.findAll('a') if (name.string != None)]
            # Extract links and replace part of string to create link with newest observations
            extracted_links = [link.get("href") for link in extracted.findAll('a') if (link.get("href") != None)]
            if (replace):
                extracted_links = [link.replace(replace_to[0], replace_to[1]) for link in extracted_links]
        except:
            extracted_names = []
            extracted_links = []

        return extracted_names, extracted_links

    # Prepare pages range
    def prepare_range(self, pages_names: List[str]) -> range:
        """Preparing the range of pages to create links

        Parameters
        ----------
        pages_names: list
            links to individual city districts
        Returns
        ------
        range
            range of pages at morizon for specific page_name
        """

        # if length is 0 then there is only 1 page
        if len(pages_names) != 0:
            last_page = int(pages_names[len(pages_names) - 1])
        else:
            last_page = 1

        return range(1, last_page + 1)

    # Create splits to relieve RAM memory
    def create_split(self, links: object, split_size: object) -> object:
        """Create splits to relieve RAM memory

        Parameters
        ----------
        links: list
            list with list based on which length of splits is created
        split_size: int
            value divided by total number of links it is used to create splits to relieve RAM memory
        Returns
        ------
        list
            list with ranges
        """

        if (len(links) < split_size):
            splitted = [[0, len(links)]]
        else:
            splitted = np.array_split(list(range(0, len(links))), len(links) / split_size)
            splitted = [[elements[0] - 1, elements[-1]] if elements[0] != 0 else [elements[0], elements[-1]] for
                        elements in splitted]
            splitted[len(splitted) - 1][1] += 1


        return splitted

    # Flatten a list
    def flatten(self, result_to_flatt: List[List[str]]) -> Union[List[List[str]],List[str]]:
        """Flatten a list

        Parameters
        ----------
        result_to_flatt: list
            which has to be flatten

        Returns
        ------
        list
            flatten list
        """

        rt = []
        for i in result_to_flatt:
            if isinstance(i, list):
                rt.extend(self.flatten(i))
            else:
                rt.append(i)
        return rt

    # General function to scrape links that activates ThreadPoolExecutor
    def scraping_all_links(self, func: Callable, all_links: List[str]) -> List[DefaultDict[str, str]]:
        """General function to scrape links that activates ThreadPoolExecutor

        Parameters
        ----------
        func: function
            function which will be activated in ThreadPoolExecutor
        all_links: list
            list with links to scrape
        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        threads = min(self.max_threads, len(all_links))

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results = list(executor.map(func, all_links))

        return results

    # Scrape missed offers and pages links
    def missed_offers_pages(self, links: List[str], offers: bool,
                            func: Callable) -> Tuple[List[DefaultDict[str, str]], List[str]]:
        """Scrape missed offers and pages links

        Parameters
        ----------
        links: list
            missing links
        offers: boolean
            determines whether the missing links relate to properties
        func: function
            function which will be activated in ThreadPoolExecutor

        Returns
        ------
        defaultdict, list
            1. scraped missed links
            2. links that are still missing
        """

        links = self.scraping_all_links(func, links)

        # Assign missed links to variable
        if offers:
            missed_links = [offers for offers in links if "page" in offers]
        else:
            missed_links = [offers for offers in links if "page" not in offers]

        return links, missed_links

    # Scrape omitted data until you have scraped all
    def missed_links_all(self, missed_offers: List[str], func: Callable, details: bool, restriction: int = 5,
                         offers: bool = None, func_pages_or_offers: Callable = None) -> List:
        """General function to scrape missing links that activates ThreadPoolExecutor until all are scraped

        Parameters
        ----------
        missed_offers: list
            missing links
        func: function
            function which will be activated in ThreadPoolExecutor
        details: boolean
            determines whether the missing links relate to details
        restriction: int
            restriction for while loop
        offers: boolean, default(None)
            determines whether the missing links relate to properties
        func_pages_or_offers: function, default(None)
            function to scrape pages or offers

        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        missed_offers_list = []
        n_times = 0

        # If there are some missed links left scrape them
        while (len(missed_offers) != 0) & (n_times <= restriction):
            if details:
                missed_scraped, missed_offers = func(missed_offers)
            else:
                missed_scraped, missed_offers = func(missed_offers, offers, func_pages_or_offers)
            missed_offers_list.append(missed_scraped)
            n_times += 1


        return missed_offers_list

    # Join missed information with already scraped
    def join_missed_with_scraped(self, missed: List[str], scraped: List[str]) -> List:
        """Join missed information with already scraped

        Parameters
        ----------
        missed: list
            scraped missed links
        scraped: list
            links scraped without problems

        Returns
        ------
        list
            scraped elements: details, and links e.g. pages
        """

        if len(missed) > 1:
            missed = [properties for properties in self.flatten(missed) if properties != None]
            scraped = np.concatenate([self.flatten(scraped), missed], axis=0)
        elif len(missed) == 1:
            scraped = np.concatenate([self.flatten(scraped), self.flatten(missed[0])], axis=0)
        elif len(missed) == 0:
            scraped = self.flatten(scraped)

        return scraped

    # Try to connect with offer link, if it is not possible save link to global list
    def scraping_offers_details_exceptions(self, link: str) -> Union[DefaultDict[str, str], str]:
        """Try to connect with offer link, if it is not possible save link to global list

        Parameters
        ----------
        link: str
           offer link

        Returns
        ------
        defaultdict or str
            If scraping succeeds, it is the details of the flat and otherwise a link to the offer
        """

        try:
            offer_infos = self.scraping_offers_details(link)
        except:
            offer_infos = link

        return offer_infos

    # Find in Beautifulsoup with 3 args
    def soup_find_information(self, soup: BeautifulSoup, find_attr: List[str]) -> List[str]:
        """Find in soup with 3 args

        Parameters
        ----------
        soup: str
            offer link
        find_attr: list
            attributes of tag

        Returns
        ------
        list
            elements with specific attributes
        """

        return soup.find(find_attr[0], attrs={find_attr[1]: find_attr[2]})

    # Extract strings from information founded in soup
    def extract_information(self, find_in: BeautifulSoup, find_with_obj: bool = False,
                            obj: str = None) -> Union[List[str], str]:
        """Find in soup with 3 args

        Parameters
        ----------
        find_in: BeautifulSoup
            object where used to find information
        find_with_obj: boolean, (default False)
            determines whether user wants to find elements by "obj"
        obj: str, (default None)
            find all elements with that object

        Returns
        ------
        list or str
            1. elements with specific attributes
            2. "None" informs that information is not available
        """

        try:
            if find_with_obj:
                return [info_part.string.strip() for info_part in find_in.find_all(obj) if (info_part.string != None)]
            else:
                return [info_part.string.strip() for info_part in find_in if (info_part.string != None)]
        except:
            return None
