import pandas as pd


class CountryManager:

    url = ...

    def download(self):
        ...
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

    cm = CountryManager()

    cm.download().harmonized()
    cm.get_raw_data()

    cm.harmonized()
