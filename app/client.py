from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import logging
import time

# Настройка логирования
logger = logging.getLogger("Kafka client")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
# Для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Для сохранения в файл
#log_filename = f"/app/logs/client.log"
#file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
#file_handler.setFormatter(formatter)
#logger.addHandler(file_handler)

# Конфигурация Schema Registry
schema_registry_config = { 'url': 'http://schema-registry:8081' }
# Инициализация клиента Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_config)

# Параметры консьюмера
consumer_config = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "group.id": "debezium-consumer-group",
    "auto.offset.reset": "earliest",
    # Настройка для ручного коммита сообщений
    "enable.auto.commit": False,
    # Настройки размера вычитки и максимального времени ожидания для вычитки по 1 сообщению
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 100
}

# Создание двух консьюмеров для разных топиков
topic1 = "customers.public.users"
consumer1 = Consumer(consumer_config)

topic2 = "customers.public.orders"
consumer2 = Consumer(consumer_config)

logger.info(f"Клиент Kafka запущен и ожидает готовность данных")

# Проверка и ожидание готовности топиков для подписки
ready = False
while not ready:
    try:
        # Запрашиваем список топиков кластера
        cluster_metadata = consumer1.list_topics(timeout=5.0)
        existing_topics = set(cluster_metadata.topics.keys())
        # Проверяем, что топики созданы
        if topic1 in existing_topics and topic2 in existing_topics:
            ready = True
    except Exception as e:
        logger.warning(f"Ошибка при запросе метаданных: {e}")
    # Повтор проверки через 30с
    time.sleep(30)

# Подписка на топики
consumer1.subscribe([topic1])
consumer2.subscribe([topic2])

# Определение десериализации ключа и значения с учётом параметров kafka-connect (CONNECT_KEY_CONVERTER, CONNECT_VALUE_CONVERTER) в docker-compose.yml
key_deserializer = AvroDeserializer(schema_registry_client)
value_deserializer = AvroDeserializer(schema_registry_client)

# Получение сообщений и вывод их в консоль
logger.info(f"Топики успешно найдены")
try:
    while True:
        # Получение сообщений из первого топика
        msg1 = consumer1.poll(0.1)
        if msg1 is not None:
            if msg1.error():
                logger.error(f"Ошибка при получении: {msg1.error()}")
            else:
                key1 = key_deserializer(msg1.key(), SerializationContext(msg1.topic(), MessageField.KEY))
                value1 = value_deserializer(msg1.value(), SerializationContext(msg1.topic(), MessageField.VALUE))
                logger.info(f"Получено сообщение из топика {msg1.topic()}: {value1}, offset={msg1.offset()}")
                # Ручной коммит после обработки сообщений
                consumer1.commit(msg1, asynchronous=False)
        # Получение сообщений из второго топика
        msg2 = consumer2.poll(0.1)
        if msg2 is not None:
            if msg2.error():
                logger.error(f"Ошибка при получении: {msg2.error()}")
            else:
                key2 = key_deserializer(msg2.key(), SerializationContext(msg2.topic(), MessageField.KEY))
                value2 = value_deserializer(msg2.value(), SerializationContext(msg2.topic(), MessageField.VALUE))
                logger.info(f"Получено сообщение из топика {msg2.topic()}: {value2}, offset={msg2.offset()}")
                # Ручной коммит после обработки сообщений
                consumer2.commit(msg2, asynchronous=False)
except Exception as e:
    logger.error(f"Произошла критическая ошибка: {e}")
finally:
    # Закрытие консьюмеров
    consumer1.close()
    consumer2.close()
