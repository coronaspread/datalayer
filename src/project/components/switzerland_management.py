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
NEW DATA:
- there is a date and time column (date get converted into time_report, time is left as is)
- added indicators:
  release_total (slightly different from recovery in meaning)
  vent_total (not sure what to do with this)
- region code 'FL' id assume this is for "federal level" but idk for sure, left it as a separate region
  
OLD SOURCE
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

# https://kb.bullseyelocations.com/support/solutions/articles/5000695305-switzerland-canton-codes
canton_code_to_name_dict = {
    "AG" : "Aargau",
    "AR" : "Appenzell Ausserrhoden",
    "AI" : "Appenzell Innerrhoden",
    "BL" : "Basel-Landschaft",
    "BS" : "Basel-Stadt",
    "BE" : "Bern",
    "FR" : "Fribourg",
    "GE" : "Geneva",
    "GL" : "Glarus",
    "GR" : "Graubünden",
    "JU" : "Jura",
    "LU" : "Luzern",
    "NE" : "Neuchâtel",
    "NW" : "Nidwalden",
    "OW" : "Obwalden",
    "SH" : "Schaffhausen",
    "SZ" : "Schwyz",
    "SO" : "Solothurn",
    "SG" : "St. Gallen",
    "TG" : "Thurgau",
    "TI" : "Ticino",
    "UR" : "Uri",
    "VS" : "Valais",
    "VD" : "Vaud",
    "ZG" : "Zug",
    "ZH" : "Zürich",
    'FL' : np.nan
}

#OLD SOURCE: data_url_path = "https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/"
#OLD SOURCE: raw_data_file_name = "covid_19_cases_switzerland_standard_format.csv"
data_url_path = "https://raw.githubusercontent.com/openZH/covid_19/master/"
raw_data_file_name = "COVID19_Fallzahlen_CH_total.csv"

class SwitzerlandManager():

    def __init__(self):
        self.data_hash = None
        self.data_harmonized = None

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        output_file = os.path.join(raw_data_dir_path, timestamp + raw_data_file_name)
        urllib.request.urlretrieve(data_url_path + raw_data_file_name, output_file)

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
            time_report : the time from when the report was uploaded (date)
            time_report_clock_time : the time from when the report was uploaded (time)
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
            'released_total'
            'deaths_total'
            'hospitalized_total'
            'intensive_care_total'
            'vent_total',
            'performed_tests_total'
        """

        data = self.get_raw_data()

        data_hash = hash(tuple(pd.util.hash_pandas_object(data)))

        if data_hash != self.data_hash:

            data.rename(columns={'abbreviation_canton_and_fl': 'region_code',
                                 'date': 'time_report',
                                 'time': 'time_report_clock_time',
                                 'ncumul_deceased' : 'deaths_total',
                                 'ncumul_tested' : 'performed_tests_total',
                                 'ncumul_conf' : 'positive_total',
                                 'ncumul_hosp' : 'hospitalized_total',
                                 'ncumul_ICU' : 'intensive_care_total',
                                 'ncumul_released' : 'released_total',
                                 'ncumul_vent' : 'vent_total'},
                        inplace=True)
            data['country_name'] = 'Switzerland'
            data['region_name'] = 'Canton'

            id_vars = ['time_report', 'time_report_clock_time', 'country_name', 'region_code', 'source']


            value_vars = [
              'performed_tests_total', 'positive_total', 'hospitalized_total',
              'intensive_care_total', 'vent_total',
              'released_total', 'deaths_total', 
            ]
            data = pd.melt(frame=data, value_vars=value_vars, id_vars=id_vars, var_name='value_type', value_name='value')

            data['region_code_native'] = data['region_code']
            data['region_code_native'] = data['region_code_native'].apply(lambda c : canton_code_to_name_dict[c])
            data.loc[data.region_code == 'FL', 'region_name'] = 'FL'

            self.data_hash = data_hash
            self.data_harmonized = data

        return self.data_harmonized
