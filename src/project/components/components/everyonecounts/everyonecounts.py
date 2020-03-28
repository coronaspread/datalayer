import requests
import json
import pandas as pd

BASE_URL = 'https://f3fp7p5z00.execute-api.eu-central-1.amazonaws.com/test/sdd-lambda-request'

if __name__ == '__main__':
    r = requests.get(BASE_URL)
    data = []
    for date, tmp in json.loads((json.loads(r.content.decode('utf-8')))['body'])['body'].items():
        for k, v in tmp.items():
            data.append({**v, **{'date': date, 'location': k}})
    df = pd.DataFrame(data)
    df.to_csv('everyonecounts.csv', index=False)
