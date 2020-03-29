

### harmonizer not finished yet

import pandas as pd
import CountryMangager

class SwitzerlandMangaer():
    BASE_URL_SWITZERLAND = "https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid_19_cases_switzerland_standard_format.csv"

def download(self):
    self.aut = pd.read_csv(BASE_URL_SWITZERLAND)
    return self

def get_raw_data(self) -> pd.DataFrame:
    '''

    :return: the raw data dataframe
    '''
    self.aut.to_csv("switzerland.csv", encoding="utf-8-sig", index=False)
    return self.aut

def harmonized(): 
    '''

    :return: the harmonized dataframe
    '''
    
    # change data format
    che_covid["date"] = pd.to_datetime(che_covid["date"]).dt.date

    # reshape from wide to long 
    id_vars = ['date', 'country', 'abbreviation_canton', 'name_canton', 'lat', 'long']
    value_vars = [
        'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized',
        'home_confinment', 'total_currently_positive_cases', 'new_positive_cases',
        'recovered', 'deaths', 'total_positive_cases', 'tests_performed'
        ]
    che_covid_melt = pd.melt(frame=che_covid, value_vars=value_vars, id_vars=id_vars, var_name="type")

    # rename columns
    che_covid_melt = che_covid_melt.rename(
        columns = {
            "country":"source", "abbreviation_canton":"region_code","name_canton":"region_code_native"
            })
        
    # paste empty columns to fit global format
    che_covid_melt["reporting_date"] = np.nan
    che_covid_melt["country_iso"] = "che"
    che_covid_melt["gender"] = np.nan
    che_covid_melt["age"] = np.nan

    che_prepared = che_covid_melt[
        ["source","reporting_date","date","country_iso",
        "region_code","region_code_native","lat","long","gender","age","type","value"]
    ]

#    che_prepared.to_csv("./datasets/switzerland_covid.csv", index=False)

    return che_prepared