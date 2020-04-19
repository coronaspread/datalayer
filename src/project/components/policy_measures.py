from io import StringIO
import requests
import pandas as pd

def get_df_from_google_sheets(sheet_url):
    response = requests.get(sheet_url)
    assert response.status_code == 200, 'Wrong status code'

    df = pd.read_csv(StringIO(response.content.decode('utf-8')))

    # delete the seconde row
    df.drop(df.head(1).index, inplace=True)  # drop first n rows

    # change data format
    df["Start"] = pd.to_datetime(df["Start"]).dt.date
    df["End"] = pd.to_datetime(df["End"]).dt.date

    #print(df.dtypes)

    df.to_csv("Policy_Measures.csv", index = False, header=True, encoding="utf-8", sep = ";")
    return df


if __name__ == '__main__':
    df = get_df_from_google_sheets('https://docs.google.com/spreadsheets/d/1tYfD3vvSEaW3Cq9-UZoDtlKIfaMMVJq8XnV9XJUdK3s/export?gid=0&format=csv')
    print(df)