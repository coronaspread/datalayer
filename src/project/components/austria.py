import pandas as pd

url = "https://opendata.arcgis.com/datasets/123014e4ac74408b970dd1eb060f9cf0_4.csv"

df = pd.read_csv(url, sep=",")

df.to_csv("austria.csv", encoding="utf-8-sig", index=False)
