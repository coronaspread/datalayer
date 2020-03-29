import pandas as pd
import numpy as np

# Github Repository: https://github.com/openZH/covid_19
BASE_URL_SWITZERLAND = "https://raw.githubusercontent.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total.csv"

che_covid_raw = pd.read_csv(BASE_URL_SWITZERLAND)

value_vars = ['ncumul_tested', 'ncumul_conf', 'ncumul_hosp', 'ncumul_ICU', 'ncumul_vent', 'ncumul_released', 'ncumul_deceased']
id_vars = che_covid_raw.columns.difference(value_vars)

che_covid_melt = pd.melt(frame=che_covid_raw, value_vars=value_vars, id_vars=id_vars, var_name="type")

dict_cantons = pd.read_csv("dict_cantons.csv")

che_covid_melt = pd.merge(
    left = che_covid_melt, left_on = "abbreviation_canton_and_fl", 
    right= dict_cantons, right_on = "abbreviation_canton")


# paste empty columns to fit global format
che_covid_melt["uuid"] = np.nan
che_covid_melt["source"] = "Switzerland"
che_covid_melt["reporting_date"] = np.nan
che_covid_melt["country_iso"] = "CHE"
che_covid_melt["gender"] = np.nan
che_covid_melt["age"] = np.nan

che_covid_melt = che_covid_melt.rename(
    columns = {"abbreviation_canton_and_fl":"region_code",
              "name_canton":"region_code_native"})

dict_type = {'ncumul_tested':"performed_tests_total", 
             'ncumul_conf':"positive_total", 
             'ncumul_hosp':"hospitalized_active", 
             'ncumul_ICU':"intensive_care_active",
             'ncumul_vent':"intensive_care_active",  
             'ncumul_released':"recovered_total", 
             'ncumul_deceased':"deaths_total"}

che_covid_melt["type"].replace(dict_type)

# rearrange the columns
harmonized_columns = ["uuid","source","reporting_date","date","country_iso","region_code","region_code_native","lat","long","gender","age","type","value"]
che_prepared = che_covid_melt[harmonized_columns]

