

import os
import pandas

import ukglobals

# note: this is the number of cumulated cases not current


columns = [ "date", "country_iso", "region_code", "region_code_native", "lat", "long", "hosp_with_symptons", "icu_case", "total_hospital", "home_insultation", "new_cummulate_positive", "discharge_and_healed", "deceased", "total_cases", "testing", "predictions", "population", "density", "over_65", "over_65_pop", "beds", "beds_per_capita", ]

data = pandas . DataFrame (columns = columns)

regional_data = pandas . read_csv (os .path . join (ukglobals . local_data_path, ukglobals . regional_confirmed_cases_file_name))


nb_regional_data_items = len (regional_data)

new_items = {}
for column_name in columns :
  new_items [column_name] = nb_regional_data_items * [ "<NA>", ]
new_items ["date"] = regional_data ["Date"] . values
new_items ["country_iso"] = nb_regional_data_items * [ "GBR", ]
new_items ["region_code"] = regional_data ["Area"] . values
new_items ["total_cases"] = regional_data ["TotalCases"] . values

data = data . append (pandas . DataFrame (new_items), sort = False)



country_data = pandas . read_csv (os .path . join (ukglobals . local_data_path, ukglobals . country_indicators_file_name))

new_items = {}

# TODO: this is probably a terrible way of doing this
for index, row in country_data . iterrows () :
  new_item = {}
  for column_name in columns :
    new_item [column_name] = "<NA>"
  new_item ["date"] = row ["Date"]
  new_item ["country_iso"] = "GBR"
  new_item ["region_code"] = row ["Country"]

  indicator = row ["Indicator"]
  if (indicator == "ConfirmedCases") :
    indicator_key = "total_cases"
  elif (indicator == "Deaths") :
    indicator_key = "deceased"
  item_key = new_item ["date"] + new_item ["region_code"]
  is_new = False
  try :
    new_items [item_key] [indicator_key] = row ["Value"]
  except (KeyError) :
    is_new = True
  if (is_new) :
    new_item [indicator_key] = row ["Value"]
    new_items [item_key] = new_item

data = data . append (pandas . DataFrame (new_items . values ()), sort = False)

data . to_csv (os . path . join (ukglobals . final_data_path, ukglobals . final_data_file_name))


  #related_rows = country_data [country_data . Date == new_item ["date"]] [country_data . Country == new_item ["region_code"]]
  
  
  
 


