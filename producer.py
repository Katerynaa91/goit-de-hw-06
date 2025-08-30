# Завдання 2. Відправка даних до топіків:

# Напишіть Python-скрипт, який імітує роботу датчика і періодично відправляє 
# випадково згенеровані дані (температура та вологість) у топік building_sensors.

# Дані мають містити ідентифікатор датчика, час отримання даних та відповідні показники.

# Один запуск скрипту має відповідати тільки одному датчику. 
# Тобто, для того, щоб імітувати декілька датчиків, необхідно запустити скрипт декілька разів.

# ID датчика може просто бути випадковим числом, але постійним (однаковим) для одного 
# запуску скрипту. При повторному запуску ID датчика може змінюватись.

# Температура — це випадкова величина від 25 до 45.
# Вологість — це випадкова величина від 15 до 85.

from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# from constants import TOPIC_BUILDING_SENSORS, TOPIC_HUMIDITY_ALERTS, TOPIC_TEMPERATURE_ALERTS

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Визначення топіку та фдентифікатора для сенсора
topic_name = kafka_config["topic_building_sensors"]
sensor_id = random.randint(1,100)

for i in range(50):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            # "timestamp": time.time(),               # час отримання даних
            "sensor_id": sensor_id,
            "temperature": random.randint(25, 45),  # температура
            "humidity": random.randint(15, 85)      # вологість
        }
        
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()                  # Очікування, поки всі повідомлення будуть відправлені
        
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()        # Закриття producer
