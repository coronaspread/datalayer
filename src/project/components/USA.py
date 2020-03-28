import pandas as pd
import numpy as np

def download():
	covid = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')
	covid.loc[covid.county=='New York City', 'fips'] = 36061
	covid = covid.dropna(subset=['fips'])
	covid.fips = covid.fips.astype(int)
	covid['state_code'] = covid.fips // 1000
	covid['county_code'] = covid.fips % 1000
	covid.columns = ['time_report','region_small_name','region_large_name','FIPS','total_positive','total_deceased','region_small_code','region_large_code']
	covid['country_iso'] = 'USA'
	covid['country_name'] = 'United States of America'
	covid['latitude'] = np.nan
	covid['longitude'] = np.nan
	counties = pd.read_csv('https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip', sep='\t', header=0, index_col=1)
	counties.columns = counties.columns.str.rstrip()
	for county in covid.FIPS.unique():
		covid.loc[covid.FIPS==county, 'latitude'] = counties.loc[county, 'INTPTLAT']
		covid.loc[covid.FIPS==county, 'longitude'] = counties.loc[county, 'INTPTLONG']
	covid = covid.drop('FIPS', axis='columns')
	covid.to_csv('usa_covid.csv')

def harmonize():
	covid = pd.read_csv('usa_covid.csv')
	value_vars = ['total_positive','total_deceased']
	id_vars = [col for col in covid.columns if col not in value_vars]
	covid_harmonized = pd.melt(covid, id_vars=id_vars, value_vars=value_vars, var_name='type')
	covid_harmonized['source'] = 'https://github.com/nytimes/covid-19-data'
	covid_harmonized.to_csv('usa_covid_harmonized.csv')

download()
harmonize()
