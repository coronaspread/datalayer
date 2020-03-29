"""
Examples
--------

    import src.project.components.uk_management as uk_management
    m = switzerland_management.SwitzerlandManager()
    m.download().get_raw_data() # download and get the raw data
    m.download().harmonized() # download and harmonize
    m.harmonized() # get the latest harmonized data
"""


"""
NOTEs:
- this data appears to be incomplete: 572 entries only, dates from 2020-03-06 to 2020-03-27
- columns:
tests_performed, hospitalized_with_symptoms, intensive_care, total_hospitalized, home_confinment, recovered, total_positive_cases
 seems to be all NaN
 (ignoring them for now)
"""

import numpy as np
import os
import pandas as pd
from datetime import datetime
import urllib.request


# pd.set_option('display.width', 700)
# pd.options.display.max_colwidth = 100
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_columns', 500)
# np.set_printoptions(linewidth=800)

raw_data_dir_path = 'data/raw/switzerland/'

data_url = "https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/"
raw_data_file_name = "covid_19_cases_switzerland_standard_format.csv"

class SwitzerlandManager():

    def __init__(self):
        self.data_hash = None
        self.data_harmonized = None

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        output_file = os.path.join(raw_data_dir_path, timestamp + raw_data_file_name)
        urllib.request.urlretrieve(data_url + raw_data_file_name, output_file)

        return self

    def get_raw_data(self):
        """

        Returns
        -------
        out : the raw data as a pandas DataFrame
        """

        timestamp_newest = datetime.strptime('1000-01-01 00-00-00.0', "%Y-%m-%d %H-%M-%S.%f")
        for root, dirs, filenames in os.walk(raw_data_dir_path):
            for filename in filenames:
                timestamp_current = datetime.strptime(" ".join(filename.split('_', 2)[:2]), "%Y-%m-%d %H-%M-%S.%f")
                if timestamp_current > timestamp_newest:
                    timestamp_newest = timestamp_current

        timestamp = timestamp_newest.strftime("%Y-%m-%d_%H-%M-%S.%f_")
        data= pd.read_csv(os.path.join(raw_data_dir_path, timestamp + raw_data_file_name))

        return data

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

        data = self.get_raw_data()

        data_hash = hash(tuple(pd.util.hash_pandas_object(data)))

        if data_hash != self.data_hash:

            data.rename(columns={'name_canton': 'area_name',
                                 'abbreviation_canton': 'area_code',
                                 'date': 'time_report',
                                 'country' : 'country_name',
                                 'deaths' : 'deaths_total'},
                        inplace=True)
            data['positive_total'] = np.nan
            data['performed_test_total'] = np.nan

            id_vars = ['time_report', 'country_name', 'area_name', 'area_code', 'lat', 'long']


            value_vars = [
              'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized',
              'home_confinment', 'total_currently_positive_cases', 'new_positive_cases',
              'recovered', 'deaths_total', 'total_positive_cases', 'tests_performed'
            ]
            # if needed remove the all-Nan columns
            #data.drop(['tests_performed', 'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized', 'home_confinment', 'recovered', 'total_positive_cases'], axis=1, inplace=True)
            data = pd.melt(frame=data, value_vars=value_vars, id_vars=id_vars, var_name='value_type', value_name='value')


            self.data_hash = data_hash
            self.data_harmonized = data

        return self.data_harmonized
