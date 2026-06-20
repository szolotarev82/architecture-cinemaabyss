## Задание 1

Описана архитектура целевого микросервисного решения (а не переходного варианта).

- [Ссылка на C4 диаграмму контейнеров](arch/c4.puml)

## Задание 2

### Proxy

Сервис-прокси реализован на Golang в src/microservices/proxy.

### Kafka

Сервис для событий реализован на Golang в src/microservices/events.

- [Скриншот прогона тестов](img/kafka_tests.png)
- [Скриншот топиков Kafka](img/kafka_topics.png)

## Задание 3

### CI/CD

Github Actions реализованы в .github/workflows.

### Proxy в Kubernetes

Kubernetes часть реализована в src/kubernetes.
Скриншоты:

- [Лог событий Kafka после прогона тестов](img/events.png)
- [Результат https://cinemaabyss.example.com/api/movies](img/kuber_movies.png)

# Задание 4

Helm часть реализована в src/kubernetes/helm. Скриншоты:

- [Логи развертывания](img/helm_start.png)
- [Результат https://cinemaabyss.example.com/api/movies](img/helm_movies.png)
