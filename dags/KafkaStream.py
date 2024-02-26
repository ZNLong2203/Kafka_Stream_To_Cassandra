import uuid
import time
import json
import requests
import logging
from datetime import datetime
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_data():
    res = requests.get("https://randomuser.me/api/")
    data = res.json()
    return data['results'][0]

def format_data(res):
    data = {}
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(res['location']['street']['number'])} {res['location']['street']['name']}, " \
                      f"{res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 120: #2 minutes
            break
        try:
            res = get_data()
            data = format_data(res)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error("Error")
            break

default_args = {
    'owner' : 'Zkare',
    'start_date' : datetime(2024, 1, 1)
}

dags = DAG(
    'Pipeline',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
)

streaming_task = PythonOperator(
    task_id = 'stream_data_from_api',
    python_callable = stream_data,
    dag = dags
)




