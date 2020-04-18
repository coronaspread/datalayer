import pandas as pd
import numpy as np

import src.general.exceptions as exept
from src.project.components.austria_management import AustriaManager
from src.project.components.china_management import ChinaManager
from src.project.components.france_management import FranceManager
from src.project.components.germany_management import GermanyManager
from src.project.components.italy_management import ItalyManager
from src.project.components.netherlands_management import NetherlandsManager
from src.project.components.switzerland_management import SwitzerlandManager
from src.project.components.uk_management import UKManager
from src.project.components.usa_management import USAManager
from pandas.api.types import is_datetime64_any_dtype as is_datetime
import re

columns = [
    'uuid',
    'source',
    'time_report',
    'time_database',
    'time_downloaded',
    'country_name',
    'country_code',
    'region_name',
    'region_code',
    'region_code_native',
    'area_name',
    'area_code',
    'area_code_native',
    'latitude',
    'longitude',
    'gender',
    'age',
    'value_type',
    'value',
    'is_new_case',
    'is_new_death']

value_types = ['positive_total',
               'positive_active',
               'positive_new',
               'recovered_total',
               'recovered_new',
               'deaths_total',
               'deaths_new',
               'hospitalized_total',
               'hospitalized_active',
               'hospitalized_new',
               'hospitalized_with_symptoms_total',
               'hospitalized_with_symptoms_active',
               'hospitalized_with_symptoms_new',
               'intensive_care_total',
               'intensive_care_active',
               'intensive_care_new',
               'confined_total',
               'confined_active',
               'confined_new',
               'performed_tests_total',
               'performed_tests_active',
               'performed_tests_new']

country_managers = {
    'austria': (AustriaManager, 'Austria'),
    'china': (ChinaManager, 'China'),
    'france': (FranceManager, 'France'),
    'germany': (GermanyManager, 'Germany'),
    'italy': (ItalyManager, 'Italy'),
    'netherlands': (NetherlandsManager, 'Netherlands'),
    'switzerland': (SwitzerlandManager, 'Switzerland'),
    'uk': (UKManager, 'United Kingdom'),
    'usa': (USAManager, 'United States of America')
}


class CountryManager:

    def __init__(self, country):
        self.country = country
        self.raw_data_hash = None
        self.data_harmonized = None

        if country not in country_managers.keys():
            raise exept.IllegalArgumentException('The country "' + country + '" is not valid. ' +
                                                 'Valid countries are ' + str(country_managers.keys()) + '.')

        self.country_name = country_managers[country][1]
        self.country_manager = country_managers[country][0]()

    def download(self):
        self.country_manager.download()
        return self

    def raw_data(self) -> pd.DataFrame:
        """

        Returns
        -------
        out : the raw data as a pandas DataFrame
        """
        return self.country_manager.raw_data()

    def harmonized(self) -> pd.DataFrame:
        """

        Returns
        -------
        out : pandas DataFrame,
        where each row is the report of a number of cases in the value column,
        missing columns mean, that all values of that column would be missing.
        The columns are:

            uuid : str id of the report
            source : link to or description of the source
            time_report : the time from when the report was uploaded
            time_database : the time from when the database was uploaded
            time_downloaded : the time when the database was downloaded
            country_name : the name of the country
            country_code : the iso code of the country
            region_name : the name of the region, e.g. "Bundesland" in Germany or "Kanton" in Switzerland
            region_code : the code of the region
            region_code_native : the code in the native language
            area_name : the name of the area, e.g. "Kreis" or "City" in Germany
            area_code : the code in the area
            area_code_native : the code in the native language
            latitude : float
            longitude : float
            gender : 'F', 'M'
            age : float or string like "30-40"
            value_type : str (see below for more information)
            value : int
            is_new_case : boolean
            is_new_death : boolean

        Notes
        -----
        The column `value_type` can have the following values:
            'positive_total'
            'positive_active'
            'positive_new'
            'recovered_total'
            'recovered_new'
            'deaths_total'
            'deaths_new'
            'hospitalized_total'
            'hospitalized_active'
            'hospitalized_new'
            'hospitalized_with_symptoms_total'
            'hospitalized_with_symptoms_active'
            'hospitalized_with_symptoms_new'
            'intensive_care_total'
            'intensive_care_active'
            'intensive_care_new'
            'confined_total'
            'confined_active'
            'confined_new'
            'performed_tests_total'
            'performed_tests_active'
            'performed_tests_new'
        """

        raw_data = self.country_manager.raw_data()
        raw_data_hash = self.country_manager.raw_data_hash(raw_data)

        if self.raw_data_hash != raw_data_hash:
            raw_data_function = self.country_manager.raw_data
            self.country_manager.raw_data = lambda: raw_data
            data_harmonized = self.country_manager.harmonized()
            self.country_manager.raw_data = raw_data_function
            self.raw_data_hash = raw_data_hash
            self.data_harmonized = data_harmonized

        invalid_columns = [column_name for column_name in self.data_harmonized.columns if column_name not in columns]
        if invalid_columns:
            raise exept.InvalidDataModel('The dataset for the country ' + self.country + ' is not correctly harmonized. \n' +
                                         'The columns ' + str(invalid_columns) + ' are invalid')

        all_strings = lambda c: c.apply(type).eq(str).all()
        all_strings_or_nan = lambda c: c.apply(lambda e: isinstance(e, str) or np.isnan(e)).all()
        all_int_range = lambda c: c.apply(lambda e: isinstance(e, (int, np.integer)) or (isinstance(e, str) and re.fullmatch(r'\d+-\d+', e))).all()
        is_float = lambda c: c.dtype == 'float64'
        is_boolean = lambda c: c.dtype == 'float64'

        column_check_functions = {
            'uuid': all_strings,
            'source': all_strings,
            'time_report': is_datetime,  # https://strftime.org/
            'time_database': is_datetime,
            'time_downloaded': is_datetime,
            'country_name': lambda c: all_strings(c) and (c == self.country_name).all(),
            'country_code': lambda c: all_strings(c) and len(c.unique()) == 1,
            'region_name': all_strings_or_nan,
            'region_code': all_strings_or_nan,
            'region_code_native': all_strings_or_nan,
            'area_name': all_strings_or_nan,
            'area_code': all_strings_or_nan,
            'area_code_native': all_strings_or_nan,
            'latitude': is_float,
            'longitude': is_float,
            'gender': lambda c: all_strings(c.unique()) and (c.unique() == ['F', 'M']).all(),
            'age': all_int_range,
            'value_type': lambda c: c.apply(lambda e: e in value_types).all(),
            'value': all_int_range,
            'is_new_case': is_boolean,
            'is_new_death': is_boolean
        }

        issue_string = ''
        for column_name in self.data_harmonized.columns:
            column = self.data_harmonized[column_name]
            column_is_valid = column_check_functions[column_name]
            # print(column)
            if not column_is_valid(column):
                if not issue_string:
                    issue_string = 'The dataset for the country ' + self.country + ' is not correctly harmonized.'
                if column_name == 'country_code':
                    issue_string = issue_string + '\n The column ' + column_name + ' has multiple isos.'
                else:
                    invalid_elements = list(np.unique([element for element in column if not column_is_valid(pd.Series(element))]))
                    issue_string = issue_string + '\n The column ' + column_name + ' has the invalid values ' + str(invalid_elements) + '.'
        if issue_string:
            raise exept.InvalidDataModel(issue_string)

        return self.data_harmonized


if __name__ == '__main__':
    # from src.project.components.uk_management import UKManager
    # from src.project.components.country_management import CountryManager

    import pandas as pd
    import numpy as np

    pd.set_option('display.width', 700)
    pd.options.display.max_colwidth = 100
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 500)
    np.set_printoptions(linewidth=800)


    cm = CountryManager('uk')
    cm.harmonized()
