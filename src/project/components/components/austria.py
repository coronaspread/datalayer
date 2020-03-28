import pandas as pd

def download():
    BASE_URL_AUSTRIA = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"
    aut = pd.read_csv(BASE_URL_AUSTRIA, sep=",")
    aut.to_csv("austria.csv", encoding="utf-8-sig", index=False)

    
def harmonize():
    # Create DataFrame:
    BASE_URL_AUSTRIA = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"
    aut = pd.read_csv(BASE_URL_AUSTRIA, sep=",")

    # Drop not needed columns:
    aut2 = aut.drop(["OBJECTID", "infizierte_pro_ew", "zuwachs", "zuwachs_prozent", "einwohner"], axis=1)

    # Rename columns:
    colnames = ["region_small_code_native", "region_small_name", "region_large_code_native", "region_large_name",
               "no_cases", "Shape_Length", "Shape_Area", "report_date"]
    aut2.columns = colnames

    # Add columns with known information:
    aut2["uuid"] = ""
    aut2["source"] = "Austria"
    aut2["country_name"] = "Austria"
    aut2["country_iso"] = "AUT"

    # Reorder columns:
    aut2 = aut2[["uuid", "source", "report_date", "country_name", "country_iso", "region_large_name", "region_large_code_native",
                "region_small_name", "region_small_code_native", "Shape_Length", "Shape_Area", "no_cases"]]
    
    # Save CSV:
    aut2.to_csv("austria_harmonized.csv", encoding="utf-8-sig", index=False)
    
    
# TODO: change datetime, check geodata (latitude/longitude)
