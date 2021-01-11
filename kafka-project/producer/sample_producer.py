import time
from json import dumps
from time import sleep

from kafka import KafkaProducer
from numpy.random import choice, randint


def get_random_query():
    new_dict = {}

    university_list = ['MSU', 'HSE', 'MIPT']
    item_list = ['Mathematics', 'English', 'Physics', 'Programming']

    new_dict['university'] = choice(university_list)
    new_dict['subject'] = choice(item_list)
    new_dict['score'] = randint(1, 10)

    return new_dict


if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             api_version=(0, 11, 5),
                             compression_type='gzip')

    my_topic = 'test_topic'

    while True:
        for _ in range(10):
            data = get_random_query()

            try:
                future = producer.send(topic=my_topic, value=data)
                record_metadata = future.get(timeout=10)

                print('--> The message has been sent to a topic: {}, partition: {}, offset: {}' \
                      .format(record_metadata.topic,
                              record_metadata.partition,
                              record_metadata.offset))

            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))

            finally:
                producer.flush()

        sleep(10)

    producer.close()
