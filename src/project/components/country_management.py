import pandas as pd
import numpy as np

from src.project.components.uk_management import UKManager
import src.general.exceptions as exept

def docstring_parameter(*sub):
    def dec(obj):
        obj.__doc__ = obj.__doc__.format(*sub)
        return obj

    return dec


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

class CountryManager:

    def __init__(self, country):
        self.country_name = country
        if country == 'uk':
            self.country_manager = UKManager()

    def download(self):
        self.country_manager.download()
        return self

    def get_raw_data(self) -> pd.DataFrame:
        """

        Returns
        -------
        out : the raw data as a pandas DataFrame
        """
        return self.country_manager.get_raw_data()

    def harmonized(self) -> pd.DataFrame:
        """

        Returns
        -------
        out : pandas DataFrame,
        where each row is the report of a number of cases in the value column,
        missing columns mean, that all values of that column would be missing.
        The columns are:

            uuid : id of the report
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
        data_harmonized = self.country_manager.harmonized()

        invalid_columns = [column_name for column_name in data_harmonized.columns if column_name not in columns]
        if invalid_columns:
            raise exept.InvalidDataModel(
                'The dataset for the country ' + self.country_name + ' is not correctly harmonized.' +
                'The columns ' + str(invalid_columns) + ' are invalid')

        invalid_value_types = list(np.unique([value_type for value_type in data_harmonized['value_type'] if value_type not in value_types]))
        if invalid_value_types:
            raise exept.InvalidDataModel(
                'The dataset for the country ' + self.country_name + ' is not correctly harmonized.' +
                'The column value_type ' + str(invalid_value_types) + ' are invalid')

        return data_harmonized


if __name__ == '__main__':

    # from src.project.components.country_management import CountryManager

    pd.set_option('display.width', 700)
    pd.options.display.max_colwidth = 100
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 500)
    np.set_printoptions(linewidth=800)

    cm = CountryManager('uk')
    data_harmonized = cm.harmonized()

    cm.download().harmonized()
    cm.get_raw_data()

    cm.harmonized()
