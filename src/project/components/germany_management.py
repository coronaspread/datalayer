import requests
import json
import pandas as pd
import os
import numpy as np
from datetime import datetime

# url = "https://api.public.fusionbase.io/cases/latest"
url = "https://api.public.fusionbase.io/cases"
headers = {
    'X-API-Key': 'd20ca43d-9626-43e4-a304-8ff59feec044'
}
# 'd20ca43d-9626-43e4-a304-8ff59feec044'

raw_data_dir_path = 'data/raw/germany/'


class GermanyManager:

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        output_file = os.path.join(raw_data_dir_path, timestamp + "covid-19-cases-germany.csv")
        response = requests.request("GET", url, headers=headers)
        data = json.loads(response.text.encode('utf8'))
        df = pd.DataFrame(data)
        df.to_csv(output_file)
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
        data_per_region = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + "covid-19-cases-germany.csv"))
        return data_per_region

    @staticmethod
    def raw_data_hash(raw_data):
        return hash(tuple(pd.util.hash_pandas_object(raw_data)))

    def harmonized(self) -> pd.DataFrame:

        data_per_region = self.raw_data()
        data_per_region['source'] = url
        data_per_region['country_name'] = 'Germany'
        data_per_region = data_per_region. \
            rename(columns={'fb_id': 'uuid',
                            'value_type': 'positive_total',
                            'cases': 'value',
                            'location_label': 'area_code_native',
                            'publication_datetime': 'time_report',
                            'fb_datetime': 'time_database',
                            'bundesland_name': 'region_name',
                            'bundesland_ags': 'region_code',
                            'kreis_name': 'area_name',
                            'kreis_ags': 'area_code'}). \
            drop(['cases_per_population', 'cases_per_100k', 'population', 'kreis_nuts', 'Unnamed: 0'], axis=1)
        data_per_region['region_code'] = data_per_region.region_code.apply(str)
        data_per_region['area_code'] = data_per_region.area_code.apply(str)
        data_per_region['time_report'] = pd.to_datetime(data_per_region.time_report, format='%Y-%m-%dT%H:%M:%S')
        data_per_region['time_database'] = pd.to_datetime(data_per_region.time_database, format='%Y-%m-%dT%H:%M:%S.%f')

        return data_per_region
