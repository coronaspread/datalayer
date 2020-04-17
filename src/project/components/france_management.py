import pandas as pd
import os
import re
import requests
import urllib
from urllib.parse import urlparse


class FranceManager:

    def __init__(self):
        self.url = 'https://www.data.gouv.fr/fr/datasets/donnees-des-urgences-hospitalieres-et-de-sos-medecins-relatives-a-lepidemie-de-covid-19/'

    def download(self):
        r = requests.get(self.url)
        matches = re.search('"(https://static.data.gouv.fr/resources/donnees-des-ugences-hospitalieres-et-de-sos-medecins-relatives-a-lepidemie-de-covid-19/[^.]+sursaud-covid[^.]+\.xlsx)"', r.content.decode('utf-8'))
        if matches is not None:
            url = matches[1]
            print(f"url is {url}")
            a = urlparse(url)
            filename = os.path.basename(a.path)
            print(f"saving file as {filename}")
            urllib.request.urlretrieve(url, filename)
        return self

    def get_raw_data(self) -> pd.DataFrame:
        '''

        :return: the raw data dataframe
        '''
        return self.download()


    def harmonized(self) -> pd.DataFrame:
        '''

        :return: the harmonized dataframe
        '''
        pass


if __name__ == '__main__':

    cm = FranceManager()

    #cm.download().harmonized()
    #cm.get_raw_data()

    #cm.harmonized()
