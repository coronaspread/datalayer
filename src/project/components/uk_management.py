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
import numpy as np
from datetime import datetime
import urllib.request

raw_data_dir_path = 'data/raw/uk/'

data_url_head = "https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/"
country_indicators_file_name = "covid-19-indicators-uk.csv"
regional_confirmed_cases_file_name = "covid-19-cases-uk.csv"


class UKManager:

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        for data_file_name in [country_indicators_file_name, regional_confirmed_cases_file_name]:
            output_file = os.path.join(raw_data_dir_path, timestamp + data_file_name)
            data_url = data_url_head + data_file_name
            urllib.request.urlretrieve(data_url, output_file)

        return self

    @staticmethod
    def raw_data():

        timestamp_newest = datetime.strptime('1000-01-01 00-00-00.0', "%Y-%m-%d %H-%M-%S.%f")
        for root, dirs, filenames in os.walk(raw_data_dir_path):
            for filename in filenames:
                timestamp_current = datetime.strptime(" ".join(filename.split('_', 2)[:2]), "%Y-%m-%d %H-%M-%S.%f")
                if timestamp_current > timestamp_newest:
                    timestamp_newest = timestamp_current

        timestamp = timestamp_newest.strftime("%Y-%m-%d_%H-%M-%S.%f_")
        data_per_region = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + country_indicators_file_name))
        data_per_area = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name))

        return data_per_region, data_per_area

    @staticmethod
    def raw_data_hash(raw_data):
        return hash((tuple(pd.util.hash_pandas_object(raw_data[0])), tuple(pd.util.hash_pandas_object(raw_data[1]))))

    def harmonized(self) -> pd.DataFrame:

        data_per_region, data_per_area = self.raw_data()

        data_per_area = data_per_area.rename(columns={"TotalCases": "Value"})
        data_per_area['Indicator'] = 'TotalCases'

        data_merged = pd.concat([data_per_region, data_per_area], sort=True).reset_index(drop=True)

        data_merged.rename(columns={'Area': 'area_name',
                                    'AreaCode': 'area_code',
                                    'Country': 'region_name',
                                    'Date': 'time_report',
                                    'Indicator': 'value_type',
                                    'Value': 'value'},
                           inplace=True)

        data_merged.value_type.replace(to_replace={'ConfirmedCases': 'positive_total',
                                                   'Deaths': 'deaths_total',
                                                   'Tests': 'performed_tests_total',
                                                   'TotalCases': 'positive_total'},
                                       inplace=True)
        data_merged['country_name'] = 'United Kingdom'
        data_merged['value'] = data_merged.value. \
            replace(to_replace=r'(\d+) ?to ?(\d+)', value=r'\1-\2', regex=True). \
            apply(pd.to_numeric, errors='ignore')
        data_merged['region_name'] = data_merged.region_name. \
            replace(to_replace=r'UK', value=np.nan, regex=True)

        return data_merged
