###COVID19 Data Preparation Netherland
import pandas as pd

# Get data for cases by region
corona_nld_region = pd.read_csv("https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_corona_in_nl.csv")

# new column with type value
corona_nld_region["type"] = "new_positive_cases"

# Abspeichern der angepassten Daten - später weglassen
#df.to_csv(Path("data", "rivm_corona_in_nl_update.csv"), index = False, columns = ["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal", "type"])

# get data with death for whole netherlands
corona_deaths = pd.read_csv("https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_corona_in_nl_fatalities.csv")

# add new columns, so that all tables have the same columns
corona_deaths["Gemeentenaam"] = "Netherlands"
corona_deaths["Gemeentecode"] = "9999"
corona_deaths["Provincienaam"] = "Netherlands"
corona_deaths["type"] = "deaths"

# change order of columns
corona_deaths = pd.DataFrame(df2, columns = ["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal", "type"])

# show death in negatives to understand better
corona_deaths["Aantal"] = corona_deaths["Aantal"] * -1

# get data from hospitalized people
corona_hospital = pd.read_csv("https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_corona_in_nl_hosp.csv")

#add new columns, so that all tables have the same columns
corona_hospital["Gemeentenaam"] = "Netherlands"
corona_hospital["Gemeentecode"] = "9998"
corona_hospital["Provincienaam"] = "Netherlands"
corona_hospital["type"] = "total_hospitalized"

# change order of columns
corona_hospital = pd.DataFrame(corona_hospital, columns = ["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal", "type"])

# get data for all positives cases in netherland
corona_allPositive = pd.read_csv("https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_corona_in_nl_daily.csv")

# add new columns, so that all tables have the same columns
corona_allPositive["Gemeentenaam"] = "Netherlands"
corona_allPositive["Gemeentecode"] = "9998"
corona_allPositive["Provincienaam"] = "Netherlands"
corona_allPositive["type"] = "total_positive_cases"

# change order of columns
corona_allPositive = pd.DataFrame(df5, columns = ["Datum", "Gemeentenaam", "Gemeentecode", "Provincienaam", "Aantal", "type"])

# merge the datasets
nld_merge = pd.concat([corona_nld_region, corona_deaths, corona_hospital, corona_allPositive], ignore_index=True)

# change columnnames
nld_merge.rename(columns = {"Gemeentenaam":"region_large_code", "Datum":"reporting_date", "Aantal":"value", "Gemeentecode":"region_code_native", "Provincienaam":"province_name", "Table Names":"source", "type":"type"}, inplace = True)

# add new columns which are required
nld_merge["uuid"] = "NA"
nld_merge["date"] = "NA"
nld_merge["country_iso"] = "NL"
nld_merge["latitude"] = "NA"
nld_merge["longitude"] = "NA"
nld_merge["gender"] = "all"
nld_merge["age"] = "all"
nld_merge["country"] = "Netherlands"

# change columnnames
nld_merge = pd.DataFrame(nld_merge, columns = ["uuid", "source", "reporting_date", "date", "country", "country_iso", "region_large_code", "region_code_native", "province_name", "latitude", "longitude", "gender", "age", "type", "value"])

print(nld_merge)
# new csv-file
nld_merge.to_csv("Corona_NL_update.csv", index = False, columns = ["uuid", "source", "reporting_date", "date",
                                                                   "country", "country_iso",
                                                                   "region_large_code",
                                                                   "region_code_native",
                                                                   "province_name", "latitude", "longitude",
                                                                   "gender", "age", "type", "value"])


