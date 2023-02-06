import requests
import json
import schedule

def get_number_of_hits_elastic():
    r = requests.get(url="http://ip_addr:9200/phishtank/_count",
                     headers={"kbn-xsrf": "reporting", 'Content-Type': 'application/json'}
                     , verify=False)
    if r.status_code == 201 or r.status_code == 200:
        dataJson = r.text
        data = json.loads(dataJson)
        count = data.get("count")
        print("Number of records:")
        print(count)
    else:
        print("cant get value: " + r.text)
    return count


def get_table_of_phish_id(number_of_records):
    list_of_phish_id_already_in_database = []
    for x in range(1,number_of_records+1):
        r = requests.get(url="http://ip_addr:9200/phishtank/_doc/" + str(x),
                         headers={"kbn-xsrf": "reporting", 'Content-Type': 'application/json'}#, data=json.dumps(x),
                         ,verify=False)
        if r.status_code == 201 or r.status_code == 200:
            dataJson = r.text
            data = json.loads(dataJson)
            data_from_phishTank = data.get("_source")
            phish_id = data_from_phishTank.get('phish_id')
            list_of_phish_id_already_in_database.append(phish_id)
        else:
            print("cant get phish_id: " + r.text)

    return list_of_phish_id_already_in_database


headers = {'User-Agent': 'phishtank'}

url = "http://data.phishtank.com/data/online-valid.json"

counter = 1

def getData(url):
    r = requests.get(url)
    dataJson = r.text
    data = json.loads(dataJson)
    return data

def saveToElastic(data, counter, table_of_phish_id):
    for x in data:
        if x.get("phish_id") not in table_of_phish_id:
            r = requests.post(url="http://ip_addr:9200/phishtank/_doc/" + str(counter),
                         headers={"kbn-xsrf": "reporting", 'Content-Type': 'application/json'}, data=json.dumps(x),
                         verify=False)
            if r.status_code == 201 or r.status_code == 200:
                counter = counter + 1
            else:
                print("cant save data,  body: " + r.text)

    print(" Data inserting complited")



def job():
    number_of_records = get_number_of_hits_elastic()
    table_of_phish_id = get_table_of_phish_id(number_of_records)
    saveToElastic(getData(url), number_of_records+1, table_of_phish_id)
    return

schedule.every().day.at("07:00").do(job)


while True:
    schedule.run_pending()


