import requests
import pandas as pd
import numpy as np
import io


def get_data_italy():
    # data per region
    CSV_URL1 = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province.csv"
    csv_data1 = requests.get(CSV_URL1).content
    df = pd.read_csv(io.StringIO(csv_data1.decode('utf-8')), sep=',')

    # data per symptoms
    CSV_URL3 = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv"
    csv_data3 = requests.get(CSV_URL3).content
    df3 = pd.read_csv(io.StringIO(csv_data3.decode('utf-8')), sep=',')


    # rename of the columns
    df.rename(columns = {"data":"report_date", "stato":"country_code", "codice_regione":"region_code",
                         "denominazione_regione":"region_name", "codice_provincia":"area_code",
                         "denominazione_provincia":"area_name", "sigla_provincia":"area_code_native",
                         "lat":"latitude", "long":"longitude", "totale_casi":"value"}, inplace = True)

    df3.rename(
        columns={"data": "report_date", "stato": "country_code", "ricoverati_con_sintomi": "hospitalized_with_symptoms",
                 "terapia_intensiva": "intensive_care", "totale_ospedalizzati": "total_hospitalized",
                 "isolamento_domiciliare": "home_confinment",
                 "totale_attualmente_positivi": "total_currently_positive_cases",
                 "nuovi_attualmente_positivi": "new_positive_cases", "dimessi_guariti": "recovered",
                 "deceduti": "deaths",
                 "totale_casi": "total_positive_cases", "tamponi": "tests_performed"}, inplace=True)


    # reshape symptoms data from wide to long format
    id_vars = ['report_date', 'country_code']
    value_vars = [
        'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized',
        'home_confinment', 'total_currently_positive_cases', 'new_positive_cases',
        'recovered', 'deaths', 'total_positive_cases', 'tests_performed'
    ]
    data = pd.melt(frame=df3, value_vars=value_vars, id_vars=id_vars, var_name="value_type")


    # adding necessary columns
    df["value_type"] = "total_positive_cases"

    data["region_name"] = np.nan
    data["region_code"] = np.nan
    data["area_name"] = np.nan
    data["area_code"] = np.nan
    data["area_code_native"] = np.nan
    data["latitude"] = np.nan
    data["longitude"] = np.nan


    # merging data
    it_merge = pd.concat([df, data], ignore_index=True)

    # adding necessary columns
    it_merge["uuid"] = np.nan # hier ID einfügen
    it_merge["time_database"] = np.nan
    it_merge["time_downloaded"] = np.nan
    it_merge["country_name"] = "Italy"
    it_merge["source"] = "https://github.com/pcm-dpc/COVID-19/blob/master/README.md"
    it_merge["region_code_native"] = np.nan
    it_merge["gender"] = np.nan
    it_merge["age"] = np.nan
    it_merge["is_new_case"] = it_merge["value_type"] == "new_positive_cases"
    it_merge["is_new_death"] = it_merge["value_type"] == "deaths"

    # splitting Date column in date and time
    new = it_merge["report_date"].str.split("T", expand=True)
    it_merge["report_date"] = new[0].copy()
    it_merge["report_date"] =pd.to_datetime(it_merge["report_date"]).dt.date
    it_merge["time_report"] = new[1].copy()

    # deleting not necessary columns
    it_merge.drop(columns=["note_it", "note_en"], inplace=True)


    # creating csv file
    it_merge.to_csv("ITA_output.csv", index=False, encoding="utf-8", header=True,
                         columns=["uuid", "source", "time_report", "time_database", "time_downloaded", "country_name",
                                    "country_code", "region_name", "region_code", "region_code_native",
                                  "area_name", "area_code", "area_code_native", "latitude", "longitude",
                                  "gender", "age", "value_type", "value", "is_new_case",
                                  "is_new_death"])

    return it_merge

get_data_italy()
