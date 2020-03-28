import os
import re
import requests
import urllib
from urllib.parse import urlparse

BASE_URL = 'https://www.data.gouv.fr/fr/datasets/donnees-des-urgences-hospitalieres-et-de-sos-medecins-relatives-a-lepidemie-de-covid-19/'

if __name__ == '__main__':
    r = requests.get(BASE_URL)
    matches = re.search('"(https://static.data.gouv.fr/resources/donnees-des-ugences-hospitalieres-et-de-sos-medecins-relatives-a-lepidemie-de-covid-19/[^.]+sursaud-covid[^.]+\.xlsx)"', r.content.decode('utf-8'))
    if matches is not None:
        url = matches[1]
        print(f"url is {url}")
        a = urlparse(url)
        filename = os.path.basename(a.path)
        print(f"saving file as {filename}")
        urllib.request.urlretrieve(url, filename)
