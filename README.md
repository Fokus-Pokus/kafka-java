# Kafka Relay Service (Spring Boot)

Сервис читает JSON-сообщения из входного Kafka-топика, вытаскивает нужные поля и отправляет сформированный JSON в выходной топик.

## Что делает
- Подключается к Kafka.
- Читает сообщения из `app.kafka.input-topic`.
- Позволяет ограничить скорость вычитки через `app.kafka.max-messages-per-second` (сообщений/сек).
- Маппит поля по конфигу `app.mapping.fields`.
- При необходимости извлекает ключ из `app.mapping.key-path`.
- Отправляет результат в `app.kafka.output-topic`.

## Конфигурация Kafka (SASL/SSL + JKS)
Основные параметры в `application.yml`:
- `sasl.mechanism: PLAIN`
- `security.protocol: SASL_SSL`
- `ssl.truststore.location`
- `ssl.truststore.password`
- `ssl.truststore.type: JKS`

> В вашем сообщении указан `SASL_SLL`, но в Kafka корректное значение — `SASL_SSL`.

## Пример входного сообщения
```json
{
  "messageId": "abc-123",
  "payload": {
    "eventType": "USER_CREATED",
    "user": {
      "id": "u-42"
    }
  }
}
```

## Пример конфига маппинга
```yaml
app:
  mapping:
    key-path: messageId
    fields:
      requestId: messageId
      userId: payload.user.id
      eventType: payload.eventType
```

## Пример выходного сообщения
```json
{
  "requestId": "abc-123",
  "userId": "u-42",
  "eventType": "USER_CREATED",
  "processedAt": "2026-02-26T12:00:00Z"
}
```

## Запуск
```bash
mvn spring-boot:run
```

Или:
```bash
mvn clean package
java -jar target/kafka-relay-service-0.0.1-SNAPSHOT.jar
```


## Ограничение скорости вычитки
- `app.kafka.max-messages-per-second: 0` — без ограничений (по умолчанию).
- `app.kafka.max-messages-per-second: 10` — не более 10 сообщений в секунду на инстанс сервиса.

Переменная окружения: `KAFKA_MAX_MESSAGES_PER_SECOND`.
