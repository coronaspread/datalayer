import requests, json, csv

def download_file_from_google(destination):
    URL = "https://api.rootnet.in/covid19-in/unofficial/covid19india.org "

    session = requests.Session()
    response = session.get(URL, params={'id': id}, stream=True)
    save_data(response, destination)


def save_data(response, destination):
    CHUNK_SIZE = 10000

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)


def read_json(filename):
    return json.loads(open(filename).read())


def write_csv(data, filename):
    with open(filename, 'wb+') as out:
        csvRows = []
        csvFileObj = open(data)
        readerObj = csv.reader(csvFileObj)
        for row in readerObj:
            if readerObj.line_num == 1:
                continue  # skip first row
            csvRows.append(row)
        csvFileObj.close()


if __name__ == "__main__":
    destination = './dataset_file.csv'
    download_file_from_google(destination)

    # write_csv(read_json(destination), 'output.csv')
