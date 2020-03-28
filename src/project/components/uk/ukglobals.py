country_indicators_file_name = "covid-19-indicators-uk.csv"
regional_confirmed_cases_file_name = "covid-19-cases-uk.csv"

# TODO: this will have to be changed to the dataset directory
local_data_path = "."


final_data_path = "."
final_data_file_name = "uk.csv"

column_names = [ "date", "country_iso", "region_code", "region_code_native", "lat", "long", "hosp_with_symptons", "icu_case", "total_hospital", "home_insultation", "new_cummulate_positive", "discharge_and_healed", "deceased", "total_cases", "testing", "predictions", "population", "density", "over_65", "over_65_pop", "beds", "beds_per_capita", ]

# TODO: fill this once the column names is fixed
date_name_key = "date"
#country_name_key = ""
region_name_key = "region_code"
cumulated_deaths_key = "deceased"
cumulated_cases_key = "total_cases"
