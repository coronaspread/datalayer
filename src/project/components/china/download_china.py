import pandas as pd 
import numpy as np


def donwload_china_raw(url= None):
    if url is None: 
        url='https://raw.githubusercontent.com/beoutbreakprepared/nCoV2019/master/latest_data/latestdata.csv'
    df = pd.read_csv(url, index_col= None, sep=',', error_bad_lines=True)
