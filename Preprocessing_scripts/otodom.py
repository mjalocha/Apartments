

import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from textwrap import wrap
from langdetect import detect
import pandas as pd
import numpy as np
import re
import os
from datetime import datetime
from textwrap import wrap
from langdetect import detect
class Preprocessing_Otodom:
    """
        A class used to preprocess offers information from Otodom.pl.
        ...
        Attributes
        ----------
        apartment_details: name of apartments table
        information_types: columns of apartments table
        Methods
        -------
        remove_quotation_marks() -> pd.DataFrame:
            Remove quotation marks from columns.
        numeric_information() -> pd.DataFrame:
            Replace numeric information to float.
        remove_new_line_marks() -> pd.DataFrame:
            Remove new line marks from columns.
        prepare_table_information(table) -> pd.DataFrame:
            Change table information from list to dictionary and create external table from it.
        get_number(price_information: str) -> str:
            Get only numeric information of price from string.
        extract_price(apartment_details_price_table) -> pd.DataFrame:
            Extract price and currency from a string and convert price to float.
        prepare_additional_info(apartment_details_add_info_table, apartment_details_details_table) -> pd.DataFrame:
            Join additional information and details to additional information.
        prepare_description_table(apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
            Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        create_table() -> pd.DataFrame:
            Create final preprocessing table.
        """
    def __init__(self, apartment_details, information_types):
        """
        Parameters
        ----------
        apartment_details : PandasDataFrame
            name of apartments table.
        information_types : str
            columns of apartments table.
        """
        self.apartment_details = apartment_details
        self.information_types = information_types

    def remove_quotation_marks(self) -> pd.DataFrame:
        """Remove quotation marks from columns.
         Parameters
         ----------
         information_types: str
             names of columns from which quotation marks need to be removed.
         apartment_details : pd.DataFrame
             data frame with information about apartments.
         Returns
         ------
         apartment_details: pd.DataFrame
             data frame with information about apartments.
         """
        information_types = self.information_types
        apartment_details = self.apartment_details
        for information_type in information_types:
            for index in range(len(apartment_details)):
                if type(apartment_details.loc[:, information_type][index])==list:
                    apartment_details.loc[:, information_type][index] = list(filter(lambda x: x != "", apartment_details.loc[:, information_type][index]))
                    try:
                        apartment_details.loc[:, information_type][index] = ', '.join(apartment_details.loc[:, information_type][index])
                    except:
                        continue
                else:
                    continue
        return apartment_details


    def numeric_information(self) -> pd.DataFrame:
        """Change numeric information to float.
        Parameters
        ----------
        information_types: str
            names of columns from which quotation marks need to be removed
        apartment_details: pd.DataFrame
            data frame with information about apartments.
        Returns
        ------
        apartment_details: pd.DataFrame
            data frame where numeric information type was changed to float.
        """
        information_types = self.information_types
        apartment_details = self.remove_quotation_marks()
        for position, information_type in enumerate(information_types):
            try:
              [float(apartment_details.loc[:, information_type][index]) for index in range(len(apartment_details))]
            except:
              continue
        return apartment_details


    def remove_new_line_marks(self) -> pd.DataFrame:
        """Remove new line marks from columns.
        Parameters
        ----------
        information_types: str
            names of columns from which new line marks need to be removed.
        apartment_details: pd.DataFrame
            data frame with information about apartments.
        Returns
        ------
        apartment_details: pd.DataFrame
            data frame where new line marks was removed.
        """
        information_types = self.information_types
        apartment_details = self.numeric_information()
        for information_type in information_types:
            for index in range(len(apartment_details)):
                try:
                    apartment_details.loc[:, information_type][index] = apartment_details.loc[:, information_type][
                        index].replace('\n\n', ', ').replace(', ,', ',').replace('\\xa0',' ')
                except:
                    continue
        return apartment_details

    def prepare_table_information(self, table: pd.DataFrame) -> pd.DataFrame:
        """Change table information from list to dictionary and create external table from it.
        Parameters
        ----------
        table: pd.DataFrame
            column with table information.
        Returns
        ------
        prepared_tables: pd.DataFrame
            data frame with table information.
        """
        prepared_table = []
        params_table = pd.DataFrame()
        for index,row in enumerate(table):

          try:
            list_info = row.replace(":", ", ").replace(" ,","").split(", ")
            to_append = dict([x for x in zip(*[iter(list_info)]*2)])
            prepared_table.append(to_append)
          except:
              prepared_table.append(None)

        for i in range(len(prepared_table)):
            column = []
            row = []
            try:
                for key, value in prepared_table[i].items():
                    column.append(key.strip(':'))
                    row.append(value)
            except:
                row.append(None)
            df_temp = pd.DataFrame([row], columns=column)
            params_table = pd.concat([params_table, df_temp], ignore_index=True)

        return params_table.where(pd.notnull(params_table),None)

    def get_number(self, price_information: str) -> str:
        """Get only numeric information of price from string.
        Parameters
        ----------
        price_information: str
            string with numeric and string price information.
        Returns
        ------
        price_number: str
            string with numeric price information.
        """
        signs = [',','.']
        if price_information in signs:
          price_number = price_information
        else:
          price_number = str.isdigit(price_information)
        return price_number

    def extract_price(self, apartment_details_price_table: pd.DataFrame) -> pd.DataFrame:
        """Extract price and currency from a string and convert price to float.
        Parameters
        ----------
        apartment_details_price_table: pd.DataFrame
            column with information about price.
        Returns
        ------
        apartment_details_price_table: pd.DataFrame
            data frame where price information type was changed to float.
        """

        currency = []
        for i in range(len(apartment_details_price_table)):
          try:
            filtered_str = filter(self.get_number, ''.join(apartment_details_price_table[i]))
            only_digit = "".join(filtered_str)
            if only_digit == ""  or only_digit == None:
                apartment_details_price_table[i] = None
                currency.append(None)
            else:
                currency.append(''.join(apartment_details_price_table[i]).split()[-1])
                apartment_details_price_table[i] = float(only_digit.replace(",", "."))
          except:
            apartment_details_price_table[i] = None
            currency.append(None)
           
        self.apartment_details['currency'] = currency
        return apartment_details_price_table

    def prepare_additional_info(self, apartment_details_add_info_table: pd.DataFrame, apartment_details_details_table: pd.DataFrame) -> pd.DataFrame:
        """Join additional information and details to additional information.
        Parameters
        ----------
        apartment_details_add_info_table: pd.DataFrame
            column with additional information.
        apartment_details_details_table: pd.DataFrame
            column with details information.
        Returns
        ------
        apartment_details_add_info_table: pd.DataFrame
            data frame with additional information.
        """
        for i in range(len(apartment_details_add_info_table)):
           try:
               apartment_details_add_info_table[i] += (', ' + apartment_details_details_table[i].replace(":",": "))
           except:
               continue
        return apartment_details_add_info_table

    def prepare_description_table(self, apartment_details_description_table: pd.DataFrame) -> pd.DataFrame:
        """Split description of 4000 characters to several columns and if record has more than 16000 characters check the language and get only polish description.
        Parameters
        ----------
        apartment_details_description_table: pd.DataFrame
            column with description.
        Returns
        ------
        description_1: List
            list with tha first part of description.
        """
        description_1 = []
        description_2 = []
        description_3 = []
        description_4 = []


        for i in range(len(apartment_details_description_table)):
          desc_list = [None, None, None, None]
          if apartment_details_description_table[i]==None:
            description_splitted = None
          elif len(apartment_details_description_table[i]) > 16000:
            description = apartment_details_description_table[i]
            text = ' '.join(description.replace(",","").replace("-","").split(" ")).split()
            elements = [text[x:x+6] for x in range(0, len(text),6)]

            pl = []
            for index, element in enumerate(elements):
              element = list(map(str.lower,element))
              try:
                language = detect(" ".join(element))
              except:
                language = 'pl'
              if language =='pl':
                pl.append(" ".join(element))

            description_splitted = wrap(" ".join(pl), 4000)

          else:
              try:
                  description_splitted = wrap(apartment_details_description_table[i], 4000)
              except:
                  description_splitted = wrap(''.join(apartment_details_description_table[i]), 4000)


          try:
            for element in range(len(description_splitted)):
              desc_list[element] = description_splitted[element]
          except:
            desc_list[element] = None

          description_1.append(desc_list[0])
          description_2.append(desc_list[1])
          description_3.append(desc_list[2])
          description_4.append(desc_list[3])

        self.apartment_details['description_2'] = description_2
        self.apartment_details['description_3'] = description_3
        self.apartment_details['description_4'] = description_4

        return description_1

    def create_table(self):
        """Create final preprocessing table.
        Returns
        ------
        otodom_table: pd.DataFrame
            final preprocessing table with None instead of empty strings.
        """
        otodom_table = pd.DataFrame()
        params_tables_otodom = self.prepare_table_information(table=self.remove_new_line_marks()['details'])
        otodom_table['area'] = self.apartment_details['Area']
        otodom_table['latitude'] = self.apartment_details['lat']
        otodom_table['longitude'] = self.apartment_details['lng']
        otodom_table['link'] = self.apartment_details['link']
        otodom_table['price'] = self.extract_price(self.apartment_details['price'])
        otodom_table['currency'] = self.apartment_details['currency']
        try:
          otodom_table['rooms'] = params_tables_otodom['Liczba pokoi']
        except:
          otodom_table['rooms'] = None
        try:
          otodom_table['floors_number'] = params_tables_otodom['Liczba pięter']
        except:
          otodom_table['floors_number'] = None
        try:
          otodom_table['floor'] = params_tables_otodom['Piętro']
        except:
          otodom_table['floor'] = None
        try:
          otodom_table['type_building'] = params_tables_otodom['Rodzaj zabudowy'].str.lower()
        except:
          otodom_table['type_building']=None
        try:
          otodom_table['material_building'] = params_tables_otodom['Materiał budynku'].str.lower()
        except:
          otodom_table['material_building'] = None
        try:
          otodom_table['year'] = params_tables_otodom['Rok budowy']
        except:
          otodom_table['year'] = None
        otodom_table['headers'] = self.apartment_details['additional_info_headers']
        otodom_table['additional_info'] = self.prepare_additional_info(apartment_details_add_info_table=self.apartment_details['additional_info'], apartment_details_details_table = self.apartment_details['details'])
        otodom_table['city'] = self.apartment_details['city']
        otodom_table['address'] = self.apartment_details['address']
        otodom_table['district'] = self.apartment_details['district']
        otodom_table['voivodeship'] = self.apartment_details['voivodeship']
        otodom_table['active'] = 'Yes'
        otodom_table['scrape_date'] = str(datetime.now().date())
        otodom_table['inactive_date'] = '-'
        otodom_table['page_name'] = 'Otodom'
        otodom_table['offer_title'] = self.apartment_details['title']
        otodom_table['description_1'] = self.prepare_description_table(self.apartment_details['description'])
        otodom_table['description_2'] = self.apartment_details['description_2']
        otodom_table['description_3'] = self.apartment_details['description_3']
        otodom_table['description_4'] = self.apartment_details['description_4']
        return otodom_table.replace({"": None})

if "__name__" == "__main__":
    otodom_preprocess = Preprocessing_Otodom(apartment_details=apartments.where(pd.notnull(apartments),None), information_types=apartments.columns)

    start_time = datetime.now()
    otodom_table = otodom_preprocess.create_table()
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))

