`1.` Запуск контейнеров Docker:
```shell
docker compose up -d
```

`2.` Вывод списка всех контейнеров Docker (включая остановленные):
```shell
docker compose ps -a
```

`3.` Создаем новый "vowels"
```shell
docker exec -ti kafka /usr/bin/kafka-topics --create --topic vowels --bootstrap-server localhost:9093
```

`4.` Создаем новый "consonants"
```shell
docker exec -ti kafka /usr/bin/kafka-topics --create --topic consonants --bootstrap-server localhost:9093
```

`5.` Получаем список Топиков которые есть в Kafka брокере, доступном по адресу kafka:9092 и находящемся внутри контейнера Docker с именем kafka:
```shell
docker exec -ti kafka /usr/bin/kafka-topics --list --bootstrap-server kafka:9093
```

`10.` Останавливаем контейнеры, удаляем контейнеры, удаляем неиспользуемые тома:
```shell
docker compose stop
docker container prune -f
docker volume prune -f
```