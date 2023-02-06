import requests
import json
import datetime
from datetime import date
import schedule



def getData(url, headers):
    #print("pobieram dane")
    r = requests.get(url, headers = headers)
    dataJson = r.text
    data = json.loads(dataJson)
    #print(data)
    return data


def saveToElastic(data, counter, index):
    #print("rozpoczynam zapis do elastica")
    for x in data:
        r = requests.post(url="http://ip_addr:9200/" + index + "/_doc/" + str(counter),
                         headers={"kbn-xsrf": "reporting", 'Content-Type': 'application/json'}, data=json.dumps(x),
                         verify=False)
        if r.status_code == 201 or r.status_code == 200:
            #print("Data saved!")
            counter = counter + 1
        else:
            print("cant save data,  body: " + r.text)

    print(" Data inserting complited")
    return counter


url_bad_hosts = 'https://honeydb.io/api/bad-hosts'
filename_bad_hosts = 'bad_hosts.txt'
index_bad_hosts = 'honeydb_bad_hosts'

url_services = 'https://honeydb.io/api/services'
filename_services = 'services.txt'
index_services = 'honeydb_services'

headers = {'X-HoneyDb-ApiId': '15b3c8982dab4245882675bb7ba35d38489639ab92e17a12571a9fbdbea6f650',
           'X-HoneyDb-ApiKey': '48212896ed6f35f91b618daedd5175d7df24f2b3df274199c2e21eab8f58b4f3'}

#es = Elasticsearch('http://ip_addr:9200')
#es.info()
#es.indices.create(index= 'honeydb-bad-hosts')

def job():
    #print("rozpoczynam dzialanie")
    f = open(filename_bad_hosts, "r")
    counter = int(f.read())
    response = getData(url_bad_hosts , headers)
    counter = saveToElastic(response, counter, index_bad_hosts)
    f = open(filename_bad_hosts, "w")
    f.write(str(counter))
    f.close()
    return

def add_date_to_response(response):
    today = date.today()
    d1 = today.strftime("%Y-%m-%d")
    for i in response:
        i["date"] = d1
    return response

def job2():
    #print("rozpoczynam dzialanie")
    f = open(filename_services, "r")
    counter = int(f.read())
    response = getData(url_services , headers)
    response = add_date_to_response(response)
    counter = saveToElastic(response, counter, index_services)
    f = open(filename_services, "w")
    f.write(str(counter))
    f.close()
    return

schedule.every().day.at("12:00").do(job)
schedule.every().day.at("23:59").do(job2)
#schedule.every(30).seconds.do(job)

while True:
    schedule.run_pending()
    #time.sleep(60) # wait one minute



