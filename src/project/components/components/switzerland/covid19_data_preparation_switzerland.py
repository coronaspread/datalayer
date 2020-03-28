### Covid19 Data Preparation Switzerland

import pandas as pd


def get_data_switzerland():

    BASE_URL_SWITZERLAND = "https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid_19_cases_switzerland_standard_format.csv"

    che_covid = pd.read_csv(BASE_URL_SWITZERLAND)

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