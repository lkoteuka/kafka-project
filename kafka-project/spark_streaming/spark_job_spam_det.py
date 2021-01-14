#!/usr/bin/python3

from __future__ import print_function

import sys
import os
import shutil
import traceback
import time
import logging
import datetime

from spam_detector import SpamDetector
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import json

outputPath = '/tmp/spark/checkpoint_01'
sqlQueryPath = 'sql/university_score.sql'
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.DEBUG)
topic_name = ""


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def check_spam(text):
    if "buy" in text or "won" in text:
        return "spam"
    else:
        return "jam"


def process(time, rdd):
    print("===========-----> %s <-----===========" % str(time))

    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        # spam_detector = SpamDetector(spark)
        curr_time = datetime.datetime.now()
        rowRdd = rdd.map(lambda w: Row(curr_time=curr_time,
                                       sender_id=w['sender_id'],
                                       sms_text=w['sms_text']
                                       ,
                                       # sms_type=spam_detector.predict(w['sms_text'])))
                                       sms_type=check_spam(w['sms_text'])))

        testDataFrame = spark.createDataFrame(rowRdd)
        testDataFrame.createOrReplaceTempView("sms_text_topic")

        # Insert into DB
        try:
            testDataFrame.write \
                .format("jdbc") \
                .mode("append") \
                .option("driver", 'org.postgresql.Driver') \
                .option("url", "jdbc:postgresql://localhost:5432/docker") \
                .option("dbtable", "sms_classified") \
                .option("user", "docker") \
                .option("password", "docker") \
                .save()

        except Exception as e:
            print("--> Opps! It seems an Errrorrr with DB working!", e)
            logger.debug("--> Opps! It seems an Errrorrr with DB working!", e)

    except Exception as e:
        print("--> Opps! Is seems an Error!!!", e)
        traceback.print_exc()
        logger.debug("--> Opps! Is seems an Error!!!", e)


def createContext():
    sc = SparkContext(appName="PythonStreamingKafkaTransaction")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 2)

    broker_list, topic = sys.argv[1:]

    try:
        directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                          [topic],
                                                          {"metadata.broker.list": broker_list})
    except:
        raise ConnectionError("Kafka error: Connection refused: \
                            broker_list={} topic={}".format(broker_list, topic))

    parsed_lines = directKafkaStream.map(lambda v: json.loads(v[1]))

    # RDD handling
    parsed_lines.foreachRDD(process)

    return ssc


if __name__ == "__main__":
    time.sleep(20)
    if len(sys.argv) != 3:
        print("Usage: spark_job.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    topic_name = sys.argv[-1]

    print("--> Creating new context")
    if os.path.exists(outputPath):
        shutil.rmtree(outputPath)

    ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
