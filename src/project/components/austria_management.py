import os
import pandas as pd
from datetime import datetime
import urllib.request

raw_data_dir_path = 'data/raw/aut/'

data_url_head = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"
regional_confirmed_cases_file_name = "covid-19-cases-aut.csv"


class AustriaManager:

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
    def raw_data(self):

        timestamp_newest = datetime.strptime('1000-01-01 00-00-00.0', "%Y-%m-%d %H-%M-%S.%f")
        for root, dirs, filenames in os.walk(raw_data_dir_path):
            for filename in filenames:
                timestamp_current = datetime.strptime(" ".join(filename.split('_', 2)[:2]), "%Y-%m-%d %H-%M-%S.%f")
                if timestamp_current > timestamp_newest:
                    timestamp_newest = timestamp_current

        timestamp = timestamp_newest.strftime("%Y-%m-%d_%H-%M-%S.%f_")
        data_regional = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name))

        return data_regional

    @staticmethod
    def raw_data_hash(raw_data):
        return hash(tuple(pd.util.hash_pandas_object(raw_data)))

    def harmonized(self) -> pd.DataFrame:

        data_regional = self.raw_data()

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

        return data_regional
