import pandas as pd
import numpy as np
import os


"""
Harmonizer missing as of now
"""

class ChinaManager:

    LOCATION =  os.path.abspath('__file__').replace('__file__', '')
    DATETIME_FORMAT = '%dd.%mm.%YY'
    url= 'https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv'
    CSV_FILENAME = '{}_raw.csv'


    def download(self, url= None):
        if self.url is not None:
            self.df = self.download_china_raw(url= self.url)
        else:
            df = pd.DataFrame()
        self.columns = self.df.keys()
        self.country_data = {}
        #self.str_to_datetime(self)
        return self


    def get_raw_data(self) -> pd.DataFrame:
        '''

        :return: the raw data dataframe
        '''
        if url is None:
            url = self.url
        df = pd.read_csv(url, index_col= None, sep=',', error_bad_lines=True)
        return df

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
