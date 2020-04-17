"""
Examples
--------
    import src.project.components.uk_management as uk_management
    ukm = uk_management.UKManager()
    ukm.download().get_raw_data() # download and get the raw data
    ukm.download().harmonized() # download and harmonize
    ukm.harmonized() # get the latest harmonized data
"""

import os
import pandas as pd
from datetime import datetime
import urllib.request

# pd.set_option('display.width', 700)
# pd.options.display.max_colwidth = 100
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_columns', 500)
# np.set_printoptions(linewidth=800)

raw_data_dir_path = 'data/raw/aut/'

data_url_head = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"
regional_confirmed_cases_file_name = "covid-19-cases-aut.csv"



class AustriaManager:

    def __init__(self):
        self.data_hash = None
        self.data_harmonized = None

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        for data_file_name in [regional_confirmed_cases_file_name]:
            output_file = os.path.join(raw_data_dir_path, timestamp + data_file_name)
            data_url = data_url_head
            urllib.request.urlretrieve(data_url, output_file)

        return self

    @staticmethod
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
        data_regional = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name))

        return data_regional

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

        data_regional = self.get_raw_data()

        data_hash = hash(tuple(pd.util.hash_pandas_object(data_regional)))

        if data_hash != self.data_hash:

            data_regional.drop(columns=["OBJECTID",
                                        "infizierte_pro_ew",
                                        "zuwachs",
                                        "zuwachs_prozent",
                                        "einwohner",
                                        "Shape_Length",
                                        "Shape_Area"],
                               inplace=True)

            data_regional.rename(columns={'BKZ': 'area_code',
                                        'PB': 'region_name',
                                        'BL_KZ': 'region_code',
                                        'BL': 'region_name',
                                        'infizierte': 'value',
                                        'datum': 'time_report',},
                               inplace=True)

            data_regional.insert(5, 'value_type', 'positive_total')
            data_regional['country_name'] = 'Austria'
            data_regional['value'] = data_regional.value.apply(pd.to_numeric, errors='ignore')
            data_regional['time_report'] = data_regional.time_report. \
                replace(to_replace=r'(\d+)-(\d+)-(\d+).(\d+):(\d+):(\d+).(\d+).', \
                        value=r'\1-\2-\3', regex=True)

            self.data_hash = data_hash
            self.data_harmonized = data_regional

        return self.data_harmonized
