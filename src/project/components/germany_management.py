import requests
import json
import pandas as pd

# url = "https://api.public.fusionbase.io/cases/latest"
url = "https://api.public.fusionbase.io/cases"

headers = {
    'X-API-Key': 'd20ca43d-9626-43e4-a304-8ff59feec044'
}

response = requests.request("GET", url, headers=headers)
data = json.loads(response.text.encode('utf8'))

df = pd.DataFrame(data)
