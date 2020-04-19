import requests
import json
import pandas as pd
import os
from datetime import datetime
from src.project.components.policy_measures import get_df_from_google_sheets
import src.general.exceptions as exceptions


url_fusionbase = "https://webconnector.fusionbase.io/isos"
url_spreadsheet = "https://docs.google.com/spreadsheets/d/1tYfD3vvSEaW3Cq9-UZoDtlKIfaMMVJq8XnV9XJUdK3s/export?gid=0&format=csv"

headers = {
    'X-API-Key': 'd20ca43d-9626-43e4-a304-8ff59feec044'
}

raw_data_dir_path = 'data/raw/measurements/'

sources = ['fusionbase', 'spreadsheet']

columns = [
    'country_name',
    'country_code',
    'region_name',
    'source',
    'affected_population_percentage',
    'restriction_type',
    'restriction_additional_info',
    'date_enacted',
    'date_revoked',
    'comment'
]

class MeasurementsManager():
    """
    Management class for measurements again covid databases.
    Currently implemented Databases:
    -fusionbase
    -sandras google spreadsheet


    """

    def download(self, src="fusionbase"):

        if src not in sources:
            raise exceptions.IllegalArgumentException('The source "' + src + '" is not available. ' +
                                                      'Current sources are:' + sources + '.')

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f_")

        if not os.path.exists(raw_data_dir_path):
            os.makedirs(raw_data_dir_path)

        output_file = os.path.join(raw_data_dir_path, timestamp + "measurements-" + src + ".csv")
        if src == "fusionbase":
            response = requests.request("GET", url_fusionbase, headers=headers)
            data = json.loads(response.text.encode('utf8'))
            df = pd.DataFrame(data)
            df.to_csv(output_file)
        elif src == "spreadsheet":
            df = get_df_from_google_sheets(url_spreadsheet)
            df.to_csv(output_file)

        return self


    @staticmethod
    def raw_data(src="fusionbase"):

        if src not in sources:
            raise exceptions.IllegalArgumentException('The source "' + src + '" is not available. ' +
                                                      'Current sources are:' + sources + '.')

        timestamp_newest = datetime.strptime('1000-01-01 00-00-00.0', "%Y-%m-%d %H-%M-%S.%f")
        for root, dirs, filenames in os.walk(raw_data_dir_path):
            for filename in filenames:
                timestamp_current = datetime.strptime(" ".join(filename.split('_', 2)[:2]), "%Y-%m-%d %H-%M-%S.%f")
                if timestamp_current > timestamp_newest:
                    timestamp_newest = timestamp_current

        timestamp = timestamp_newest.strftime("%Y-%m-%d_%H-%M-%S.%f_")
        data_measurements = pd.read_csv(os.path.join(raw_data_dir_path, timestamp + "measurements-"+src+".csv"))
        return data_measurements

    @staticmethod
    def raw_data_hash(raw_data):
        return hash(tuple(pd.util.hash_pandas_object(raw_data)))

    def harmonized(self) -> pd.DataFrame:

        self.download(src="fusionbase")
        data_measurements_fusionbase = self.raw_data(src="fusionbase")
        data_measurements_fusionbase.rename(columns={'country_alpha3': 'country_code',
                                                     'restriction': 'restriction_additional_info'}, inplace=True)
        data_measurements_fusionbase.drop(['Unnamed: 0'], axis=1, inplace=True)

        self.download(src="spreadsheet")
        data_measurements_spreadsheet = self.raw_data(src="spreadsheet")
        data_measurements_spreadsheet = data_measurements_spreadsheet. \
            rename(columns={'Country': 'country_name',
                            'ISO Code': 'country_code',
                            'Region': 'region_name',
                            'Source': 'source',
                            'Affected Pop Share': 'affected_population_percentage',
                            'Type': 'restriction_type',
                            'Meta (e.g. group size thresholds': 'restriction_additional_info',
                            'Start': 'date_enacted',
                            'End': 'date_revoked',
                            'Comment': 'comment'}). \
            drop(['Unnamed: 0', 'ADM1'], axis=1)
        data_merged = data_measurements_spreadsheet.append(data_measurements_fusionbase)
        return data_merged

