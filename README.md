### Kafka-spark-postgres project

To run a project at the root of the project:

`docker-compose up -d`

When all containers are launched, you need to create a topic in kafka. To do this, go inside the kafka container:

`docker exec -ti kafka bash`

Then, let's create a topic called `test_topic`:

`kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2 --topic test_topic`