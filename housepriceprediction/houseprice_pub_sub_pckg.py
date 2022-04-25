from dataclasses import dataclass
import json
from sre_constants import ANY
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
# import urllib library
from urllib.request import urlopen
from time import sleep

# Create producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,10,1),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Read HousePrices from URL
url = 'http://127.0.0.1:5000/predict_home_price'
response = urlopen(url)
data_json = json.loads(response.read())
producer.send('houseprices', data_json[ANY])
#print(data_json["setup"])
sleep(1)

# Consumer
consumer = KafkaConsumer('houseprices',
                     bootstrap_servers=['localhost:9092'],
                     api_version=(0,11,5),
                     group_id=None,
                     enable_auto_commit=True,
                     auto_offset_reset='earliest')


