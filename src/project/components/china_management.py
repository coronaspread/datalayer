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

raw_data_dir_path = 'data/raw/china/'

url = 'https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv'

regional_confirmed_cases_file_name = "covid-19-cases-uk.csv"


class ChinaManager:

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        output_file = os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name)
        urllib.request.urlretrieve(url, output_file)
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
        data_per_region = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name))

        return data_per_region

    @staticmethod
    def raw_data_hash(raw_data):
        return hash(tuple(pd.util.hash_pandas_object(raw_data)))

    def harmonized(self, extended=False) -> pd.DataFrame:

        data_per_region = self.raw_data()
        data_per_region['value'] = 1

        data_per_region = data_per_region. \
            rename(columns={'ID': 'uuid',
                            'date_confirmation': 'time_report',
                            'country': 'country_name',
                            'city': 'area_name',
                            'province': 'region_name',
                            'sex': 'gender'}). \
            drop(['symptoms', 'lives_in_Wuhan', 'travel_history_dates', 'travel_history_location',
                  'reported_market_exposure', 'additional_information', 'chronic_disease_binary',
                  'chronic_disease', 'sequence_available', 'date_death_or_discharge',
                  'notes_for_discussion', 'location', 'admin3', 'admin2', 'admin1', 'country_new',
                  'admin_id', 'data_moderator_initials', 'travel_history_binary',
                  'date_admission_hospital', 'geo_resolution', 'date_onset_symptoms'], axis=1)
        data_per_region['gender'] = data_per_region['gender'].map({'male': "m", 'female': "f"})

        if extended:
            data_extension = pd.melt(data_per_region, id_vars=['outcome'], value_vars='value')
            data_merged = pd.concat([data_per_region, data_extension.reindex(data_per_region.index)], axis=1)
            data_dead = data_merged.query('outcome == "death"')
            data_dead['value_type'] = 'deaths_new'
            data_dead.drop(['outcome'], axis=1, inplace=True)
            data_discharge = data_merged.query('outcome == "discharge"')
            data_discharge['value_type'] = 'recovered_new'
            data_discharge.drop(['outcome'], axis=1, inplace=True)
            data_merged.drop(['outcome'], axis=1, inplace=True)
            data_merged['value_type'] = 'positive_new'
            data_merged = data_merged.append([data_dead, data_discharge])
            return data_merged

        data_per_region['value_type'] = 'positive_new'
        data_per_region.drop(['outcome'], axis=1, inplace=True)
        return data_per_region

