FROM confluentinc/cp-kafka-connect:7.7.1
# Переключаемся на root для выполнения системных команд
USER root
# Скопировать JMX Exporter в докер образ
COPY jmx_prometheus_javaagent-0.15.0.jar /opt/
# Скопировать настройки JMX Exporter в докер образ
COPY kafka-connect.yml /opt/
# Даем права пользователю appuser на чтение этих файлов
RUN chmod 644 /opt/jmx_prometheus_javaagent-0.15.0.jar && \
    chmod 644 /opt/kafka-connect.yml
# Возвращаем стандартного пользователя Confluent обратно
USER appuser
