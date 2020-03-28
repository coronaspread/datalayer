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
    df.rename(columns = {"data":"report_date", "stato":"country_iso", "codice_regione":"region_large_code",
                         "denominazione_regione":"region_large_name", "codice_provincia":"region_small_code",
                         "denominazione_provincia":"region_small_name", "sigla_provincia":"region_small_code_native",
                         "lat":"lat", "long":"long", "totale_casi":"value"}, inplace = True)

    df3.rename(
        columns={"data": "report_date", "stato": "country_iso", "ricoverati_con_sintomi": "hospitalized_with_symptoms",
                 "terapia_intensiva": "intensive_care", "totale_ospedalizzati": "total_hospitalized",
                 "isolamento_domiciliare": "home_confinment",
                 "totale_attualmente_positivi": "total_currently_positive_cases",
                 "nuovi_attualmente_positivi": "new_positive_cases", "dimessi_guariti": "recovered",
                 "deceduti": "deaths",
                 "totale_casi": "total_positive_cases", "tamponi": "tests_performed"}, inplace=True)


    # reshape symptoms data from wide to long format
    id_vars = ['report_date', 'country_iso']
    value_vars = [
        'hospitalized_with_symptoms', 'intensive_care', 'total_hospitalized',
        'home_confinment', 'total_currently_positive_cases', 'new_positive_cases',
        'recovered', 'deaths', 'total_positive_cases', 'tests_performed'
    ]
    data = pd.melt(frame=df3, value_vars=value_vars, id_vars=id_vars, var_name="type")


    # adding necessary columns
    df["type"] = "total_positive_cases"

    data["region_large_name"] = np.nan
    data["region_large_code"] = np.nan
    data["region_small_name"] = np.nan
    data["region_small_code"] = np.nan
    data["region_small_code_native"] = np.nan
    data["lat"] = np.nan
    data["long"] = np.nan


    # merging data
    it_merge = pd.concat([df, data], ignore_index=True)

    # adding necessary columns
    it_merge["source"] = "Italy"
    it_merge["region_large_code_native"] = np.nan
    it_merge["gender"] = np.nan
    it_merge["agecovid"] = np.nan

    # splitting Date column in date and time
    new = it_merge["report_date"].str.split("T", expand=True)
    it_merge["report_date"] = new[0].copy()
    it_merge["report_date"] =pd.to_datetime(it_merge["report_date"]).dt.date

    # deleting not necessary columns
    it_merge.drop(columns=["note_it", "note_en"], inplace=True)


    # creating csv file
    it_merge.to_csv("ITA_output.csv", index=False, encoding="utf-8", header=True,
                         columns=["source", "report_date", "country_iso", "region_large_name", "region_large_code",
                                           "region_large_code_native",
                                           "region_small_name", "region_small_code", "region_small_code_native", "lat",
                                           "long", "gender", "agecovid", "type", "value"])

    return it_merge

get_data_italy()
