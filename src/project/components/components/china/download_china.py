import pandas as pd 
import numpy as np
import os

class China_DF(object): 
    
    LOCATION =  os.path.abspath('__file__').replace('__file__', '')
    DATETIME_FORMAT = '%dd.%mm.%YY'
    URL= 'https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv'
    CSV_FILENAME = '{}_raw.csv'
    
    def __init__(self, url= URL):
        self.url = url
        if self.url is not None: 
            self.df = self.download_china_raw(url= self.url) 
        else: 
            df = pd.DataFrame()
        self.columns = self.df.keys()
        self.country_data = {}
#         self.str_to_datetime(self)
        
    def download_china_raw(self, url= None):    
        '''
        download the raw csv file from github
        '''
        if url is None: 
            url = self.url
        df = pd.read_csv(url, index_col= None, sep=',', error_bad_lines=True)
        return df
    
    def get_column_value_types(self, column):
        '''
        For one column, get a list of occuring data values
        e.g column = 'country' would return a list of all countries present in the dataframe
        column: str type, the column to be scanned
        '''
        cd = df[column].unique()
        cd = list(cd)
        for el in cd: 
            if not isinstance(el, (str, np.datetime64, np.number)): 
                cd.remove(el)
        cd.sort()
        return cd
    
    def create_raw_csv(self, filename = None, country= 'all'): 
        '''
        create csv file with raw data, either from the whole frame or a specific country
        country: str type, country name to be parsed to csv
        '''
        if filename is None:
            filename = China_DF.CSV_FILENAME.format(country)
        path = China_DF.LOCATION + '\\' + filename
        print(path)
        if country == 'all':
            self.df.to_csv(path_or_buf = path)
        else: 
            if not country in self.country_data: 
                self.extract_country(country = country)
            self.country_data[country].to_csv(path_or_buf = path)
            
    def extract_country(self, country ='China'):
        '''
        extract data from a single country and add it to the class dictionary
        country: str type, needs to be present in the 'country' column of self.df
        '''
        countries = self.get_column_value_types('country')
        if not country in countries: 
            raise Exception('{} not present in dataframe'.format(country))
        elif country in self.country_data:
            return
        cols = self.columns.to_list()
        cols.remove('country')
        new_df = pd.DataFrame(df.iloc[np.where(df['country']== country)], columns = cols)
        self.country_data[country] = new_df
        return 
    
    #not in a working state
    def str_to_datetime(self, format = DATETIME_FORMAT, exclude = []): 
        '''
        convert string valued dates for np.datetime64
        '''
        for column in self.df.keys():
            if column in exclude: 
                continue
            if 'date' in column: 
                df = self.df 
                self.df[column] = pd.to_datetime(df[column], format= format)


if __name__ == '__main__':
    cdf = China_DF()
    cdf.create_raw_csv()
    cdf.create_raw_csv(country='China')