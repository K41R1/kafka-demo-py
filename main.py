import urllib.request
from kafka import KafkaProducer
import time
import json

API_KEY="56c4b5f32be9b6abdc7982986a4b1f142852968f"
url="https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer=KafkaProducer(bootstrap_servers="localhost:9092")

def main():
    while True:
        response = urllib.request.urlopen(url)
        stations = json.loads(response.read().decode())
        for station in stations:
            producer.send("velib-stations", json.dumps(station).encode())
        print("{} produced {} station records".format(time.time(), len(stations)))
        time.sleep(2)

if __name__ == '__main__':
    main()
