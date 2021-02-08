from flask import Flask
from flask_mysqldb import MySQL 
from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd
import h2o
from h2o.estimators.gbm import H2OGradientBoostingEstimator
import sqlite3 


app = Flask(__name__)

con = sqlite3.connect("models.db")  
print("Database opened successfully")  
con.execute("create table if not exists model (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT NOT NULL)")  
print("Table created successfully")  

h2o.connect(ip="127.0.0.1", port=54321)

global row

model_path = ''
producer=KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),api_version=(20, 2, 1), bootstrap_servers='192.168.99.100:9092')



#@app.route('/generate')
def generate():
    dts = h2o.import_file(path="C:/Users/ahmed/Documents/stage ing/dataset10.csv")
    dts['isFraud']=dts['isFraud'].asfactor()
    y='isFraud'
    global test
    train , rest =dts.split_frame([0.4])
    test , valid =rest.split_frame([0.4])
    global model
    model = H2OGradientBoostingEstimator(distribution='bernoulli',ntrees=100,max_depth=4,learn_rate=0.1)
    x = list(train.columns)
    del x[9:12]
    model.train(x=x, y=y, training_frame=train, validation_frame=valid)
    global model_path
    model_path = h2o.save_model(model=model,path="/tmp/mymodel", force=True)
    cur = con.cursor()  
    cur.execute("INSERT into model (path) values"+" (\""+model_path+"\")")  
    con.commit()  
    return model_path

def view():  
    con = sqlite3.connect("models.db")  
    con.row_factory = sqlite3.Row  
    cur = con.cursor()  
    cur.execute("select * from model")  
    rows = cur.fetchall() 
    if not rows:
        return generate()
    else:
        return rows[0]["path"]
    

#@app.route('/predict')
def predict():
    global saved_model
    model = h2o.load_model(model_path)
    pr = h2o.H2OFrame(row)
    p = model.predict(pr)
    lp = h2o.as_list(p)
    result_dict=lp.to_dict()
    result = result_dict['predict'][0]
    row['isFraud'] = result
    print(row)
    producer.send("stop",row)
    print("ok")
    return row


consumer = KafkaConsumer('stope',bootstrap_servers=['192.168.99.100:9092'],api_version=(20, 2, 1),auto_offset_reset='earliest',group_id='my-group1',auto_commit_interval_ms=1000,enable_auto_commit=True,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in consumer:
        model_path = view()
        if model_path == '':
                generate()
        row = message.value
        predict()


#h2o.init()
if __name__ == "__main__":
    app.run(debug=True)
