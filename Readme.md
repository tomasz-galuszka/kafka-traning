**Kafka + ui:**
````
docker run --rm --net=host landoop/fast-data-dev
````

**Kafka console avro-consumer**
<br/>- Start container: ````docker run --net=host -it confluentinc/cp-schema-registry:latest bash````
<br/>- Run consumer: ````kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic consumer --from-beginning````