# Kafka Relay Service (Spring Boot)

Сервис читает JSON из Kafka, извлекает поля и отправляет трансформированный JSON в целевой топик.

## Под ваш кейс (1 пользователь, 4 топика)
Один Kafka-пользователь (`user1`) используется и как consumer, и как producer.

Маршруты:
- `route1`: topic1 (consumer `user1`) -> topic2 (producer `user1`)
- `route3`: topic3 (consumer `user1`) -> topic4 (producer `user1`)

Итого:
- topic1: user1 consumer
- topic2: user1 producer
- topic3: user1 consumer
- topic4: user1 producer

Для consumer используется один `group-id` пользователя: `app.kafka.user.group-id`.

## Безопасность Kafka
Используется:
- `sasl.mechanism: PLAIN`
- `security.protocol: SASL_SSL`
- `ssl.truststore.*` через JKS

> В исходном сообщении было `SASL_SLL`, корректное Kafka-значение: `SASL_SSL`.

## Конфигурация (пример)
```yaml
app:
  kafka:
    bootstrap-servers: localhost:9092
    security:
      protocol: SASL_SSL
      sasl-mechanism: PLAIN
      truststore-location: /opt/certs/truststore.jks
      truststore-password: secret
      truststore-type: JKS

    user:
      username: kafka-user-1
      password: pass-1
      group-id: group-user-1

    routes:
      route1:
        input-topic: topic-1
        output-topic: topic-2
        max-messages-per-second: 30
      route3:
        input-topic: topic-3
        output-topic: topic-4
        max-messages-per-second: 20

  mapping:
    key-path: messageId
    fields:
      requestId: messageId
      userId: payload.user.id
      eventType: payload.eventType
```

## Ограничение скорости вычитки
Для каждого route отдельно:
- `app.kafka.routes.<route>.max-messages-per-second: 0` — без ограничений
- `> 0` — лимит сообщений/сек на конкретный маршрут

## Запуск
```bash
mvn spring-boot:run
```
