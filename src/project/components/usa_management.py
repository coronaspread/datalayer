"""
Examples
--------

    import src.project.components.usa_management as usa_management
    usa = usa_management.USAManager()
    usa.download().get_raw_data() # download and get the raw data
    usa.download().harmonized() # download and harmonize
    usa.harmonized() # get the latest harmonized data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import urllib.request

data_url_head_covid = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/'
data_file_name_covid = 'us-counties.csv'

data_url_head_census = 'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/'
data_file_name_census = '2019_Gaz_counties_national.zip'

raw_data_dir_path = 'data/raw/usa/'


class USAManager:

    def __init__(self):
        self.data_hash = None
        self.data_harmonized = None

    def download(self):

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        for data_url_head, data_file_name in [[data_url_head_covid, data_file_name_covid], [data_url_head_census, data_file_name_census]]:
            output_file = os.path.join(raw_data_dir_path, timestamp + data_file_name)
            data_url = data_url_head + data_file_name
            urllib.request.urlretrieve(data_url, output_file)

        return self

    @staticmethod
    def get_raw_data():
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
        data_covid = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + data_file_name_covid))
        data_census = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + data_file_name_census), sep='\t', header=0, index_col=1)
        data_census.columns = data_census.columns.str.strip()

        return data_covid, data_census

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

        data_covid, data_census = USAManager.get_raw_data()

        data_hash = hash((tuple(pd.util.hash_pandas_object(data_covid)), tuple(pd.util.hash_pandas_object(data_census))))

        if data_hash != self.data_hash:

            data_covid.loc[data_covid.county == 'New York City', 'fips'] = 36061
            # data_covid = data_covid.dropna(subset=['fips'])
            data_covid['country_name'] = 'United States of America'
            data_covid['country_iso'] = 'USA'
            data_covid['fips'] = pd.Series(data_covid.fips, dtype="Int64")
            data_covid['region_code'] = data_covid.fips // 1000
            data_covid['source'] = 'https://github.com/nytimes/covid-19-data'

            data_covid.rename(columns={'date': 'time_report',
                                       'county': 'area_name',
                                       'state': 'region_name',
                                       'fips': 'area_code',
                                       'cases': 'positive_total',
                                       'deaths': 'deaths_total'},
                              inplace=True)

            data_covid['latitude'] = np.nan
            data_covid['longitude'] = np.nan
            for area_code in data_covid.area_code.unique():
                if isinstance(area_code, np.int64):
                    data_covid.loc[data_covid.area_code == area_code, ['latitude', 'longitude']] = data_census.loc[area_code, ['INTPTLAT', 'INTPTLONG']].values

            id_vars = [col for col in data_covid.columns if col not in ['positive_total', 'deaths_total']]

            self.data_hash = data_hash
            self.data_harmonized = pd.melt(data_covid, id_vars=id_vars, var_name='value_type', value_name='value')

        return self.data_harmonized
