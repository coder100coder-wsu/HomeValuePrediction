from flask import Flask, request, jsonify,render_template
import util
import jsonpickle
import json
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from time import sleep

app = Flask(__name__)

@app.route('/get_location_names', methods=['GET'])
def get_location_names():
    response = jsonify({
        'locations': util.get_location_names()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

@app.route('/predict_home_price', methods=['GET', 'POST'])
def predict_home_price():
    total_sqft = float(request.form['total_sqft'])
    location = request.form['location']
    bhk = int(request.form['bhk'])
    bath = int(request.form['bath'])
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         api_version=(0,10,1))
                         
    response = jsonify({
        'estimated_price': util.get_estimated_price(location,total_sqft,bhk,bath)
    })
 
    #print(json.loads(response.get_data().decode("utf-8")))
   
    response.headers.add('Access-Control-Allow-Origin', '*')
    
    data_value = {'Areq Sq Feet':total_sqft, 'Location':location, 'Bedrooms':bhk, 'Bathrooms':bath, "output":json.loads(response.get_data().decode("utf-8")) }
    
    producer.send('houseprices', json.dumps(data_value).encode('utf-8'))
    #producer.send('houseprices', json.loads(response.get_data().decode("utf-8")))
    producer.close()
    
    consumer = KafkaConsumer('houseprices',
                     bootstrap_servers=['localhost:9092'],
                     api_version=(0,11,5),
                     group_id=None,
                     enable_auto_commit=True,
                     auto_offset_reset='earliest')
    consumer.close()
    
    return response
    

if __name__ == "__main__":
    print("Starting Python Flask Server For Home Price Prediction...")
    util.load_saved_artifacts()
    app.run()

    
