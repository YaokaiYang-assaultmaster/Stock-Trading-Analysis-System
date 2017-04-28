## Prerequisite before running the data-producer code

1. Set up a zookeeper server in docker container

```
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper
```

2. Set up a Kafka server in docker container

```
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka
```

3. Now we can run the data-producer.py with following command

```
python data-producer.py GOOGL stock-price localhost:9092
```

Notice that there could be an error like following:

```
WARNING:apscheduler.scheduler:Execution of job "fetch_price (trigger: interval[0:00:01], next run at: 2017-04-28 01:18:07 EDT)" skipped: maximum number of running instances reached (1)
```

This is due to we set the apscheduler to run the fetching code every 1 second, but it actually need more than 1 second to run. So it's basically a network issue. 

4. Reading data from terminal
After starting the `data-producer.py` program, we could use following command in terminal to read it.

```
kafka-console-consumer.sh localhost:9092 --topic stock-price --from-beginning --zookeeper localhost:2181
```
