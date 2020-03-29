import pandas as pd

che_covid_url = "https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid_19_cases_switzerland_standard_format.csv"

che_covid = pd.read_csv(che_covid_url)

dict_cantons = che_covid[["name_canton","abbreviation_canton","lat","long"]].drop_duplicates()

dict_cantons.rename({
    "name_canton":"region_code_native",
    "abbreviation_canton":"region_code",
},
    inplace = True
)

dict_cantons.to_csv("dict_cantons.csv", index=False)