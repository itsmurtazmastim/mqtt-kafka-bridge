import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time

# MQTT broker details
broker_address = "localhost"
broker_port = 1883 # EcliMQTT 
mqtt_topic = "trucks\data"

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "mqtt client")
#mqtt_client.connect(broker_address, broker_port)

kafka_client = KafkaClient(hosts='localhost:9092')
kafka_topic = kafka_client.topics['trucks']
kafka_producer = kafka_topic.get_sync_producer()

def on_message(client, userdata, message):
    msg_payload = str(message.payload)
    print("Received MQTT message ", msg_payload)
    kafka_producer.produce(str(msg_payload).encode('ascii'))
    print("Kafka: Just published" + str(msg_payload) + "to topic trucks")

mqtt_client.on_message = on_message
mqtt_client.connect(broker_address, broker_port)
mqtt_client.subscribe(mqtt_topic)
mqtt_client.loop_forever()
print(f"Received the following message: {mqtt_client.user_data_get()}")
