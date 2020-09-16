from kafka import KafkaConsumer
import re


consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['stock_prices_predictions'])
for msg in consumer:
    symbol = re.split("\t", msg.value.decode("utf-8"))[0]
    price = re.split("\t", msg.value.decode("utf-8"))[1]
    print(f"{msg.key.decode('utf-8')}\t{symbol}\t{price}\t {msg.offset}")

