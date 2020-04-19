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

raw_data_dir_path = 'data/raw/netherlands/'

data_url_head = "https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/"
cases_file_name = "rivm_corona_in_nl.csv"
fatalities_file_name = "rivm_corona_in_nl_fatalities.csv"
hospitalized_file_name = "rivm_corona_in_nl_hosp.csv"
cases_total_file_name = "rivm_corona_in_nl_daily.csv"


class NetherlandsManager:

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        for data_file_name in [cases_file_name, fatalities_file_name, hospitalized_file_name, cases_total_file_name]:
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
        data_cases = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + cases_file_name))
        data_fatalities = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + fatalities_file_name))
        data_hospitalized = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + hospitalized_file_name))
        data_cases_total = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + cases_total_file_name))

        return data_cases, data_fatalities, data_hospitalized, data_cases_total

    @staticmethod
    def raw_data_hash(raw_data):
        return hash((tuple(pd.util.hash_pandas_object(raw_data[0])), tuple(pd.util.hash_pandas_object(raw_data[1]))))

    def harmonized(self) -> pd.DataFrame:

        data_cases, data_fatalities, data_hospitalized, data_cases_total = self.raw_data()

        data_cases["type"] = "positive_new"

        data_fatalities["Gemeentenaam"] = "Netherlands"
        data_fatalities["Gemeentecode"] = "9999"
        data_fatalities["Provincienaam"] = "Netherlands"
        data_fatalities["type"] = "deaths_total"
        data_fatalities = pd.DataFrame(data_fatalities, columns=["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal",
                                                                 "type"])

        data_hospitalized["Gemeentenaam"] = "Netherlands"
        data_hospitalized["Gemeentecode"] = "9998"
        data_hospitalized["Provincienaam"] = "Netherlands"
        data_hospitalized["type"] = "hospitalized_total"
        data_hospitalized = pd.DataFrame(data_hospitalized,
                                         columns=["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal",
                                                  "type"])
        data_cases_total["Gemeentenaam"] = "Netherlands"
        data_cases_total["Gemeentecode"] = "9998"
        data_cases_total["Provincienaam"] = "Netherlands"
        data_cases_total["type"] = "positive_total"

        data_merged = pd.concat([data_cases, data_fatalities, data_hospitalized, data_cases_total],
                                ignore_index=True)
        data_merged.rename(columns={"Datum": "time_report",
                                    "Gemeentenaam": "area_name",
                                    "Gemeentecode": "area_code",
                                    "Provincienaam": "region_name",
                                    "type": "value_type",
                                    "Aantal": "value"}, inplace=True)
        data_merged["time_report"] = data_merged["time_report"].astype(str)
        data_merged["area_code"] = data_merged["area_code"].astype(str)
        return data_merged
