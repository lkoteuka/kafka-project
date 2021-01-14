import logging
import time
from json import dumps
from time import sleep

from kafka import KafkaProducer
from numpy.random import choice, randint

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.DEBUG)


def get_random_sms(sms_texts):
    new_dict = {}

    new_dict['sms_text'] = choice(sms_texts)
    new_dict['sender_id'] = randint(1, 10)

    return new_dict


if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             api_version=(0, 11, 5),
                             compression_type='gzip')

    my_topic = 'sms_text_topic'

    sms_examples = []
    with open("sms_examples", 'r') as f:
        sms_examples = f.readlines()

    while True:
        for _ in range(10):
            data = get_random_sms(sms_examples)

            try:
                future = producer.send(topic=my_topic, value=data)
                record_metadata = future.get(timeout=10)

                print('--> The message has been sent to a topic: {}, partition: {}, offset: {}' \
                      .format(record_metadata.topic,
                              record_metadata.partition,
                              record_metadata.offset))

            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))
                logger.error("Error while sending message: " % str(e))

            finally:
                producer.flush()

        sleep(10)

    producer.close()
