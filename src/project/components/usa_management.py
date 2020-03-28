import pandas as pd
import numpy as np

import CountryManager

class USAManager():
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'

    def download(self):
        self.covid = pd.read_csv(self.url)
        self.covid.loc[self.covid.county=='New York City', 'fips'] = 36061
        self.covid = self.covid.dropna(subset=['fips'])
        self.covid.fips = self.covid.fips.astype(int)
        self.covid['state_code'] = self.covid.fips // 1000
        self.covid['county_code'] = self.covid.fips % 1000
        self.covid.columns = ['time_report','region_small_name','region_large_name','FIPS','total_positive','total_deceased','region_small_code','region_large_code']
        self.covid['country_iso'] = 'USA'
        self.covid['country_name'] = 'United States of America'
        self.covid['latitude'] = np.nan
        self.covid['longitude'] = np.nan
        counties = pd.read_csv('https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip', sep='\t', header=0, index_col=1)
        counties.columns = counties.columns.str.rstrip()
        for county in self.covid.FIPS.unique():
            self.covid.loc[self.covid.FIPS==county, 'latitude'] = counties.loc[county, 'INTPTLAT']
            self.covid.loc[self.covid.FIPS==county, 'longitude'] = counties.loc[county, 'INTPTLONG']
        self.covid = self.covid.drop('FIPS', axis='columns')
        self.covid.to_csv('usa_covid.csv')
        return self

    def get_raw_data(self) -> pd.DataFrame:
        '''

        :return: the raw data dataframe
        '''
        return self.covid

    def harmonized(self) -> pd.DataFrame:
        '''

        :return: the harmonized dataframe
        '''
        self.covid = pd.read_csv('usa_covid.csv')
        value_vars = ['total_positive','total_deceased']
        id_vars = [col for col in self.covid.columns if col not in value_vars]
        self.covid_harmonized = pd.melt(self.covid, id_vars=id_vars, value_vars=value_vars, var_name='type')
        self.covid_harmonized['source'] = 'https://github.com/nytimes/covid-19-data'
        self.covid_harmonized.to_csv('usa_covid_harmonized.csv')
        return self.covid_harmonized

if __name__ == '__main__':

    cm = USAManager()

    cm.download().harmonized()
    #cm.get_raw_data()

    #cm.harmonized()
