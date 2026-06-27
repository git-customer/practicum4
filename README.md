# Общее описание

В составе решения присутствуют следующие компоненты на базе контейнеров Docker:
- Кластер Apache Kafka из трёх узлов под управлением Kraft
- Schema Registry
- Kafka UI
- Kafka Connect
- PostgreSQL
- Prometheus
- Grafana
- Приложение Kafka client на Python, выполняющее функции consumer для вывода в консоль сообщений из топиков.  

Параметры компонентов заданы в файле docker-compose.yml. Для сохранения состояния кластера Kafka, а также других компонентов при выключении, настроены volumes на диске. А общая сеть "kafka_shared_network" позволяет отдельным контейнерам общаться между собой. Параметры приложения Python заданы в файлах app/docker-compose.yml и app/Dockerfile.

# Разворачивание инфраструктуры.

1. Установить docker по инструкции https://docs.docker.com/engine/install
2. Запустить контейнеры решения одной командой. Если не указывать ключ -d, при запуске сразу будут видны логи, по которым можно убедиться в отсутствии ошибок.  
`sudo docker compose up -d`  
Ожидаемый результат (данном примере все файлы решения были размещены на локальной машине в папке practice, поэтому имеют в названии practice):  
```
[+] up 13/13
 ✔ Image practice-grafana               Built                                                                                                                                            5.9s
 ✔ Image practice-kafka-connect         Built                                                                                                                                            5.9s
 ✔ Network docker_kafka_shared_net      Created                                                                                                                                          0.1s
 ✔ Container practice-ui-1              Started                                                                                                                                          4.3s
 ✔ Container practice-x-kafka-common-1  Started                                                                                                                                          4.3s
 ✔ Container practice-grafana-1         Started                                                                                                                                          5.0s
 ✔ Container practice-kafka-1-1         Started                                                                                                                                          4.7s
 ✔ Container postgres                   Started                                                                                                                                          5.2s
 ✔ Container practice-kafka-2-1         Started                                                                                                                                          4.4s
 ✔ Container practice-kafka-0-1         Started                                                                                                                                          4.2s
 ✔ Container practice-schema-registry-1 Started                                                                                                                                          5.1s
 ✔ Container practice-kafka-connect-1   Started                                                                                                                                          6.0s
 ✔ Container practice-prometheus-1      Started                                                                                                                                          6.7s
```
**Примечание**: Если впоследствии потребуется полностью очистить данные контейнеров чтобы запустить тестирование "с нуля", то можно использовать команду, удаляющую volumes:
`sudo docker compose down -v`


# Проверка работы компонентов в консоли сервера.

1. Вывести список запущенных контейнеров.  
`sudo docker ps`
3. Выполнить команду внутри контейнера для просмотра списка имеющихся топиков.  
`sudo docker exec -it practice-kafka-0-1 kafka-topics.sh --list --bootstrap-server kafka-0:9092`  
Ожидаемый результат:
```
__consumer_offsets
_schemas
connect-config-storage
connect-offset-storage
connect-status-storage
```  
4. Можно проверить логи конкретного контейнера и найти там значения настроенных параметров, например папок логов.  
```
sudo docker logs practice-kafka-0-1 | grep 'log.dirs ='
sudo docker logs -f practice-kafka-0-1
```
5. Проверка статуса kafka-connect.  
`curl -s localhost:8083 | jq`  
Ожидаемый результат:  
```
{
  "version": "7.7.1-ccs",
  "commit": "91d86f33092378c89731b4a9cf1ce5db831a2b07",
  "kafka_cluster_id": "CHpINAhqSmyCh8Z5JUEgzA"
}
```
6. Проверка установленных плагинов kafka-connect.  
`curl localhost:8083/connector-plugins | jq`  
Ожидаемый результат:  
```
[
  {
    "class": "io.debezium.connector.postgresql.PostgresConnector",
    "type": "source",
    "version": "3.5.2.Final"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "type": "source",
    "version": "7.7.1-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "type": "source",
    "version": "7.7.1-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "type": "source",
    "version": "7.7.1-ccs"
  }
]
```


# Проверка работы компонентов при помощи UI.

В случае обращения с ПК/сервера, на котором запущен docker, адрес будет http://localhost:<номер порта>. В составе решения WEB-интерфейс доступен для нескольких компонентов:  
1. Kafka UI для проверки доступен на порту 8085. Статус кластера должен быть online, должна отображаться информация о версии, брокерах, партициях и т.д.
2. Prometheus доступен на порту 9090. В разделе targets должны присутствовать две записи в состоянии UP - kafka-connect-host (1/1 up), prometheus (1/1 up).
3. Grafana UI доступен на порту 3000. Настройка дашборда будет описана далее.


# Подготовка окружения для тестрирования.

1. Создайте базу данных PostgreSQL и таблицы, которые будут использоваться для работы — users и orders, а также контрольную таблицу topsecret.  
Подключение к контейнеру БД:  
`sudo docker exec -it postgres psql -h 127.0.0.1 -U postgres-user -d customers`  
В открывшейся консоли выполните команды создания таблиц:  
```
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE topsecret (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    user_secret_info VARCHAR(100),
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```  
Ожидаемый результат во всех трёх случаях:  
`CREATE TABLE`  

2. Создание коннектора на базе Debezium с настройками из файла connector.json. 
В составе решения уже поставляется Self-Hosted JDBC Connector debezium-connector-postgres-3.5.2.Final-plugin. В случае необходимости использования другой версии, её можно скачать с сайта debezium.io, распаковать архив и положить в папку components\  
В данном решении Debezium Connector настраивается для отслеживания изменений только в таблицах users и orders.  
`curl -X POST -H "Content-Type: application/json" -d @connector.json http://localhost:8083/connectors | jq`  
Ожидаемый результат:  
```
{
  "name": "pg-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres-user",
    "database.password": "postgres-pw",
    "database.dbname": "customers",
    "database.server.name": "customers",
    "table.include.list": "public.users,public.orders",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "topic.prefix": "customers",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "-1",
    "topic.creation.default.partitions": "-1",
    "skipped.operations": "none",
    "snapshot.mode": "when_needed",
    "heartbeat.interval.ms": "5000",
    "topic.heartbeat.prefix": "__debezium-heartbeat",
    "name": "pg-connector"
  },
  "tasks": [],
  "type": "source"
}
```
3. Проверка, что коннектор создан и работает:  
`curl http://localhost:8083/connectors/pg-connector/status | jq`  
Ожидаемый результат:  
```
{
  "name": "pg-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "localhost:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "localhost:8083"
    }
  ],
  "type": "source"
}
```
4. Создание дашборда в Grafana.
- Откройте сервис Grafana в браузере http://localhost:3000 (имя пользователя и пароль - admin/admin - желательно впоследствии изменить).
- Загрузите дашборд через меню "Import" => "Upload JSON file", выберите файл connect.json из папки grafana/dashboards
- Задайте настройки дашборда: имя, директорию и уникальный идентификатор.


# Тестирование работы решения.

1. Запуск приложения Kafka client.  
`cd app && sudo docker compose up --build -d`  
Ожидаемый результат:    
```
[+] up 2/2
 ✔ Image app-client       Built                                                                                                                                                          7.3s
 ✔ Container app-client-1 Started                                                                                                                                                        1.3s
```
2. Проверка логов работы приложения Kafka client.  
`sudo docker logs app-client-1`  
Ожидаемый результат:  
`client-1  | 2026-06-27 05:29:13,584 INFO: Клиент Kafka запущен и ожидает готовность данных`
3. Добавление в БД тестовых данных.  
`sudo docker exec -i postgres psql -h 127.0.0.1 -U postgres-user -d customers < db_test_data.sql`  
Ожидаемый результат: будет выведено полное содержимое таблиц users, orders, topsecret, включающее добавленные строки.
4. Повторная проверка логов работы приложения Kafka client.  
`sudo docker logs app-client-1`  
Ожидаемый результат (в логах должны появиться сообщения, автоматически полученные из топиков customers.public.users, customers.public.orders):
```
2026-06-27 05:29:13,584 INFO: Клиент Kafka запущен и ожидает готовность данных
2026-06-27 05:31:13,640 INFO: Топики успешно найдены
2026-06-27 05:31:20,035 INFO: Получено сообщение из топика customers.public.users: {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'created_at': 1782538221217380}, offset=0
2026-06-27 05:31:20,181 INFO: Получено сообщение из топика customers.public.orders: {'id': 1, 'user_id': 1, 'product_name': 'Product A', 'quantity': 2, 'order_date': 1782538221228231}, offset=0
2026-06-27 05:31:20,187 INFO: Получено сообщение из топика customers.public.users: {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'created_at': 1782538221222033}, offset=1
2026-06-27 05:31:20,191 INFO: Получено сообщение из топика customers.public.orders: {'id': 2, 'user_id': 1, 'product_name': 'Product B', 'quantity': 1, 'order_date': 1782538221232145}, offset=1
2026-06-27 05:31:20,196 INFO: Получено сообщение из топика customers.public.users: {'id': 3, 'name': 'Alice Johnson', 'email': 'alice@example.com', 'created_at': 1782538221224230}, offset=2
2026-06-27 05:31:20,202 INFO: Получено сообщение из топика customers.public.orders: {'id': 3, 'user_id': 2, 'product_name': 'Product C', 'quantity': 5, 'order_date': 1782538221234557}, offset=2
2026-06-27 05:31:20,207 INFO: Получено сообщение из топика customers.public.users: {'id': 4, 'name': 'Bob Brown', 'email': 'bob@example.com', 'created_at': 1782538221226323}, offset=3
2026-06-27 05:31:20,209 INFO: Получено сообщение из топика customers.public.orders: {'id': 4, 'user_id': 3, 'product_name': 'Product D', 'quantity': 3, 'order_date': 1782538221236140}, offset=3
2026-06-27 05:31:20,315 INFO: Получено сообщение из топика customers.public.orders: {'id': 5, 'user_id': 4, 'product_name': 'Product E', 'quantity': 4, 'order_date': 1782538221238219}, offset=4
```
5. Проверка, что kafka-connect не отслеживает таблицу topsecret.  
`sudo docker exec -it practice-kafka-0-1 kafka-topics.sh --list --bootstrap-server kafka-0:9092`  
Ожидаемый результат: в списке автоматически созданных топиков с префиксом customers должен отсутствовать тописк customers.public.topsecret.  
```
__consumer_offsets
__debezium-heartbeat.customers
_schemas
connect-config-storage
connect-offset-storage
connect-status-storage
customers.public.orders
customers.public.users
```
6. Проверка метрик на дашборде Grafana.  
Ожидаемый результат:

![Screenshot 1](https://github.com/git-customer/practicum4/blob/9dcfb5f391f34e272f300dabce22796e9fc7163d/screenshots/grafana-1.png)

![Screenshot 2](https://github.com/git-customer/practicum4/blob/9dcfb5f391f34e272f300dabce22796e9fc7163d/screenshots/grafana-2.png)
