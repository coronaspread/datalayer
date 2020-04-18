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
    def raw_data():

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


    @staticmethod
    def raw_data_hash(raw_data):
        return hash((tuple(pd.util.hash_pandas_object(raw_data[0])), tuple(pd.util.hash_pandas_object(raw_data[1]))))

    def harmonized(self) -> pd.DataFrame:

        data_covid, data_census = USAManager.raw_data()

        data_covid.loc[data_covid.county == 'New York City', 'fips'] = 36061
        # data_covid = data_covid.dropna(subset=['fips'])
        data_covid['country_name'] = 'United States of America'
        data_covid['country_code'] = 'USA'
        data_covid['fips'] = pd.Series(data_covid.fips, dtype="Int64")
        data_covid['region_code'] = pd.Series(data_covid.fips // 1000)
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

        data_covid['region_code'] = data_covid.region_code.apply(str)
        data_covid['area_code'] = data_covid.area_code.apply(str)
        data_covid['time_report'] = pd.to_datetime(data_covid.time_report, format='%Y-%m-%d')

        id_vars = [col for col in data_covid.columns if col not in ['positive_total', 'deaths_total']]

        return pd.melt(data_covid, id_vars=id_vars, var_name='value_type', value_name='value')
