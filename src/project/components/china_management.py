import pandas as pd
import numpy as np
import os

import CountryManager

"""
Harmonizer missing as of now
"""

class ChinaManager(CountryManager):

    LOCATION =  os.path.abspath('__file__').replace('__file__', '')
    DATETIME_FORMAT = '%dd.%mm.%YY'
    URL= 'https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv'
    CSV_FILENAME = '{}_raw.csv'

    def __init__(self, url= URL):

        self.url = url
        if self.url is not None:
            self.df = self.download(url= self.url)
        else:
            df = pd.DataFrame()
        self.columns = self.df.keys()
        self.country_data = {}
        #self.str_to_datetime(self)

    def download(self, url= None):
        if url is None:
            url = self.url
        df = pd.read_csv(url, index_col= None, sep=',', error_bad_lines=True)
        return df

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
        pass


if __name__ == '__main__':

    cm = ChinaManager()

    #cm.download().harmonized()
    #cm.get_raw_data()

    #cm.harmonized()
