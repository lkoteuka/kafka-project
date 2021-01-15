### Kafka-spark-postgres project

To run a project at the root of the project:

`docker-compose up -d`

When all containers are launched, you need to create a topic in kafka. To do this, go inside the kafka container:

`docker exec -ti kafka bash`

Then, let's create a topic called `sms_text_topic`:

```
kafka-topics --create --bootstrap-server localhost:9092 \
--replication-factor 1 --partitions 2 --topic sms_text_topic
```

After that in others console windows run producer (from kafka-project/producer):

`python sample_sms_producer.py`

And run spark job (from kafka-project/producer):

```
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,org.postgresql:postgresql:9.4.1207 \
spark_job_spam_det.py localhost:9092 sms_text_topic
```


Another example (with averaging over RDD using sql) can be run, there is create a topic 'test_topic' and run the corresponding reducer (from kafka-project/producer):

`python sample_producer.py`

And corresponding spark job consumer:

```
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,org.postgresql:postgresql:9.4.1207 \
spark_job_univ_score.py localhost:9092 test_topic
```

Example with ML model for purchasing prediction (topic `ads_topic`):

`python sample_ads_producer.py`

```
spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,org.postgresql:postgresql:9.4.1207 \
spark_job_ads_purchase_pred.py localhost:9092 ads_topic
```