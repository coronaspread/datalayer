import pandas as pd
import CountryManager

class AustriaManager(CountryManager):

    BASE_URL_AUSTRIA = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"
    aut = pd.read_csv(BASE_URL_AUSTRIA, sep=",")

    def download(self):
        aut.to_csv("austria.csv", encoding="utf-8-sig", index=False)
        return self

    def get_raw_data(self) -> pd.DataFrame:
        '''

        :return: the raw data dataframe
        '''
        pass

    def harmonized(self) -> pd.DataFrame:
        '''

        :return: the harmonized dataframe
        '''
        aut2 = aut.drop(["OBJECTID", "infizierte_pro_ew", "zuwachs", "zuwachs_prozent", "einwohner"], axis=1)
        colnames = ["region_small_code_native", "region_small_name", "region_large_code_native", "region_large_name",
                   "no_cases", "Shape_Length", "Shape_Area", "report_date"]
        aut2.columns = colnames

        aut2["uuid"] = ""
        aut2["source"] = "Austria"
        aut2["country_name"] = "Austria"
        aut2["country_iso"] = "AUT"

        aut2 = aut2[["uuid", "source", "report_date", "country_name", "country_iso", "region_large_name", "region_large_code_native",

                    "region_small_name", "region_small_code_native", "Shape_Length", "Shape_Area", "no_cases"]]
        aut2.to_csv("austria_harmonized.csv", encoding="utf-8-sig", index=False)
        return aut2


if __name__ == '__main__':

    cm = AustriaManager()

    #cm.download().harmonized()
    #cm.get_raw_data()

    #cm.harmonized()
