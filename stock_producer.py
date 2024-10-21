import pandas as pd
import time
from kafka import KafkaProducer
import json

def read_stock_data_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    stock_data = read_stock_data_from_csv('stock_data.csv')
    
    print("Main Started")
    for index, row in stock_data.iterrows():
        message = {
            'symbol': row['symbol'],
            'Close': row['Close'],
            'timestamp': row['timestamp']
        }
        print("Iteration Started:",message)
        send_to_kafka(producer, 'stockTopic', message)
        time.sleep(1)  # Adjust the sleep time as needed

if __name__ == "__main__":
    main()
