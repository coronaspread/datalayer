import subprocess
import os
import pandas as pd
from datetime import datetime
import urllib

from src.project.components.CountryManager import CountryManager


raw_data_dir_path = 'data/raw/uk/'

class UKManager(CountryManager):

    country_indicators_file_name = "covid-19-indicators-uk.csv"
    regional_confirmed_cases_file_name = "covid-19-cases-uk.csv"

    data_url_head = "https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/"

    # TODO: this will have to be changed to the dataset directory
    local_data_path = "."

    final_data_path = "."
    final_data_file_name = "uk.csv"

    column_names = ["date", "country_iso", "region_code", "region_code_native", "lat", "long", "hosp_with_symptons", "icu_case", "total_hospital", "home_insultation", "new_cummulate_positive",
                    "discharge_and_healed", "deceased", "total_cases", "testing", "predictions", "population", "density", "over_65", "over_65_pop", "beds", "beds_per_capita", ]

    # TODO: fill this once the column names is fixed
    date_name_key = "date"
    # country_name_key = ""
    region_name_key = "region_code"
    cumulated_deaths_key = "deceased"
    cumulated_cases_key = "total_cases"

    def _download_the_data(self, data_file_name, output_file):
        data_url = self.data_url_head + data_file_name
        try:
            # subprocess.check_call(["wget", "--no-check-certificate", "--content-disposition", data_url])
            urllib.urlretrieve(data_url, output_file)
        except (FileNotFoundError):
            subprocess.check_call(["curl", "-LJ0", data_url, "-o", data_file_name])

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")
        self._download_the_data(self.country_indicators_file_name, os.path.join(raw_data_dir_path, timestamp + self.country_indicators_file_name))
        self._download_the_data(self.regional_confirmed_cases_file_name, os.path.join(raw_data_dir_path, timestamp + self.regional_confirmed_cases_file_name))

        return self

    def get_raw_data(self) -> pd.DataFrame:
        '''

        :return: the raw data dataframe
        '''
        pass

    def harmonized(self) -> pd.DataFrame:
        '''

        :return: the harmonized dataframe
        '''
        # note: this is the number of cumulated cases not current

        columns = self.column_names

        data = pd.DataFrame(columns=columns)

        regional_data = pd.read_csv(os.path.join(self.local_data_path, self.regional_confirmed_cases_file_name))

        nb_regional_data_items = len(regional_data)

        new_items = {}
        for column_name in columns:
            new_items[column_name] = nb_regional_data_items * ["<NA>", ]
        new_items[self.date_name_key] = regional_data["Date"].values
        new_items["country_iso"] = nb_regional_data_items * ["GBR", ]
        new_items[self.region_name_key] = regional_data["Area"].values
        new_items[self.cumulated_cases_key] = regional_data["TotalCases"].values

        data = data.append(pd.DataFrame(new_items), sort=False)

        country_data = pd.read_csv(os.path.join(self.local_data_path, self.country_indicators_file_name))

        new_items = {}

        # TODO: this is probably a terrible way of doing this
        for index, row in country_data.iterrows():
            new_item = {}
            for column_name in columns:
                new_item[column_name] = "<NA>"
            new_item[self.date_name_key] = row["Date"]
            new_item["country_iso"] = "GBR"
            new_item[self.region_name_key] = row["Country"]

            indicator = row["Indicator"]
            if (indicator == "ConfirmedCases"):
                indicator_key = self.cumulated_cases_key
            elif (indicator == "Deaths"):
                indicator_key = self.cumulated_deaths_key
            item_key = new_item["date"] + new_item["region_code"]
            is_new = False
            try:
                new_items[item_key][indicator_key] = row["Value"]
            except (KeyError):
                is_new = True
            if (is_new):
                new_item[indicator_key] = row["Value"]
                new_items[item_key] = new_item

        data = data.append(pd.DataFrame(new_items.values()), sort=False)

        data.to_csv(os.path.join(self.final_data_path, self.final_data_file_name))

        # related_rows = country_data [country_data . Date == new_item ["date"]] [country_data . Country == new_item ["region_code"]]
