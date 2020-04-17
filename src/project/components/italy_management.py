"""
Examples
--------

    import src.project.components.uk_management as uk_management
    ukm = uk_management.UKManager()
    ukm.download().get_raw_data() # download and get the raw data
    ukm.download().harmonized() # download and harmonize
    ukm.harmonized() # get the latest harmonized data
"""

import os, io
import pandas as pd
import numpy as np
from datetime import datetime
import urllib.request


# pd.set_option('display.width', 700)
# pd.options.display.max_colwidth = 100
# pd.set_option('display.max_rows', 100)
# pd.set_option('display.max_columns', 500)
# np.set_printoptions(linewidth=800)

raw_data_dir_path = 'data/raw/italy/'

data_url_head1 = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/"
data_url_head2 = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-andamento-nazionale/"
regional_confirmed_cases_file_name = "dpc-covid19-ita-province.csv"
country_case_outcome_file_name = "dpc-covid19-ita-andamento-nazionale.csv"
data_url_list = [data_url_head1, data_url_head2]
data_file_list = [regional_confirmed_cases_file_name, country_case_outcome_file_name]

class ItalyManager():

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        for data_url_head, data_file_name in zip(data_url_list,data_file_list):
            output_file = os.path.join(raw_data_dir_path, timestamp + data_file_name)
            urllib.request.urlretrieve(data_url_head + data_file_name, output_file)

        return self

    def raw_data(self):

        timestamp_newest = datetime.strptime('1000-01-01 00-00-00.0', "%Y-%m-%d %H-%M-%S.%f")
        for root, dirs, filenames in os.walk(raw_data_dir_path):
            for filename in filenames:
                timestamp_current = datetime.strptime(" ".join(filename.split('_', 2)[:2]), "%Y-%m-%d %H-%M-%S.%f")
                if timestamp_current > timestamp_newest:
                    timestamp_newest = timestamp_current

        timestamp = timestamp_newest.strftime("%Y-%m-%d_%H-%M-%S.%f_")
        data_country = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + country_case_outcome_file_name))
        data_regional = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + regional_confirmed_cases_file_name))

        return data_country, data_regional

    @staticmethod
    def raw_data_hash(raw_data):
        return hash((tuple(pd.util.hash_pandas_object(raw_data[0])), tuple(pd.util.hash_pandas_object(raw_data[1]))))

    def harmonized(self) -> pd.DataFrame:

        data_country, data_regional = self.raw_data()

        data_regional.rename(columns = {"data":"report_date", "stato":"country_code", "codice_regione":"region_code",
                             "denominazione_regione":"region_name", "codice_provincia":"area_code",
                             "denominazione_provincia":"area_name", "sigla_provincia":"area_code_native",
                             "lat":"latitude", "long":"longitude", "totale_casi":"value"}, inplace = True)

        data_country.rename(columns={"data": "report_date", "stato": "country_code", "ricoverati_con_sintomi": "hospitalized_with_symptoms",
                 "terapia_intensiva": "intensive_care", "totale_ospedalizzati": "total_hospitalized",
                 "isolamento_domiciliare": "home_confinment",
                 "totale_attualmente_positivi": "total_currently_positive_cases",
                 "nuovi_attualmente_positivi": "new_positive_cases", "dimessi_guariti": "recovered",
                 "deceduti": "deaths",
                 "totale_casi": "total_positive_cases", "tamponi": "tests_performed"}, inplace=True)

        id_vars = ['report_date', 'country_code']

        value_vars = [
            'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized',
            'home_confinment', 'total_currently_positive_cases', 'new_positive_cases',
            'recovered', 'deaths', 'total_positive_cases', 'tests_performed'
        ]


        data_temp = pd.melt(frame=data_country, value_vars=value_vars, id_vars=id_vars, var_name="value_type")
        data_regional["value_type"] = "total_positive_cases"

        data_temp["region_name"] = np.nan
        data_temp["region_code"] = np.nan
        data_temp["area_name"] = np.nan
        data_temp["area_code"] = np.nan
        data_temp["area_code_native"] = np.nan
        data_temp["latitude"] = np.nan
        data_temp["longitude"] = np.nan

        it_merge = pd.concat([data_regional, data_temp], ignore_index=True)

        it_merge["uuid"] = np.nan # hier ID einfügen
        it_merge["time_database"] = np.nan
        it_merge["time_downloaded"] = np.nan
        it_merge["country_name"] = "Italy"
        it_merge["source"] = "https://github.com/pcm-dpc/COVID-19/blob/master/README.md"
        it_merge["region_code_native"] = np.nan
        it_merge["gender"] = np.nan
        it_merge["age"] = np.nan
        it_merge["is_new_case"] = it_merge["value_type"] == "new_positive_cases"
        it_merge["is_new_death"] = it_merge["value_type"] == "deaths"

        splitter = it_merge["report_date"].str.split("T", expand=True)
        it_merge["report_date"] = splitter[0].copy()
        it_merge["report_date"] =pd.to_datetime(it_merge["report_date"]).dt.date
        it_merge["time_report"] = splitter[1].copy()
        it_merge.drop(columns=["note_it", "note_en"], inplace=True)


        data_merged = it_merge

        data_merged['country_name'] = 'Italy'
        data_merged['value'] = data_merged.value. \
            replace(to_replace=r'(\d+) ?to ?(\d+)', value=r'\1-\2', regex=True). \
            apply(pd.to_numeric, errors='ignore')

        return data_merged
