#!/usr/bin/python3

from __future__ import print_function

import sys
import os
import shutil
import traceback
import time
import logging

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import json

outputPath = '/tmp/spark/checkpoint_01'
sqlQueryPath = 'sql/university_score.sql'
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.DEBUG)

def get_sql_query():
    strSQL = ''

    try:
        f = open(sqlQueryPath, 'r')
        strSQL = f.read()
        f.close()
    except Exception as e:
        print(f"-->Can't open the file {sqlQueryPath}", e)

    return strSQL


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def process(time, rdd):
    print("===========-----> %s <-----===========" % str(time))
    logger.debug("===========-----> %s <-----===========" % str(time))

    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        rowRdd = rdd.map(lambda w: Row(university=w['university'],
                                       subject=w['subject'],
                                       score=w['score']))

        testDataFrame = spark.createDataFrame(rowRdd)

        testDataFrame.createOrReplaceTempView("test_topic")

        sql_query = get_sql_query()
        testResultDataFrame = spark.sql(sql_query)
        testResultDataFrame.show(n=5)

        # Insert into DB
        try:
            testResultDataFrame.write \
                .format("jdbc") \
                .mode("append") \
                .option("driver", 'org.postgresql.Driver') \
                .option("url", "jdbc:postgresql://localhost:5432/postgres") \
                .option("dbtable", "university_score") \
                .option("user", "docker") \
                .option("password", "docker") \
                .save()

        except Exception as e:
            print("--> Opps! It seems an Errrorrr with DB working!", e)
            logger.debug("--> Opps! It seems an Errrorrr with DB working!", e)

    except Exception as e:
        print("--> Opps! Is seems an Error!!!", e)
        logger.debug("--> Opps! Is seems an Error!!!", e)


def createContext():
    sc = SparkContext(appName="PythonStreamingKafkaTransaction")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 2)

    broker_list, topic = sys.argv[1:]

    directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                      [topic],
                                                      {"metadata.broker.list": broker_list})

    # try:
    #     directKafkaStream = KafkaUtils.createDirectStream(ssc,
    #                                                       [topic],
    #                                                       {"metadata.broker.list": broker_list})
    # except:
    #     raise ConnectionError("Kafka error: Connection refused: \
    #                         broker_list={} topic={}".format(broker_list, topic))

    # directKafkaStream = None
    # while directKafkaStream is None:
    #     try:
    #         directKafkaStream = KafkaUtils.createDirectStream(ssc,
    #                                                           [topic],
    #                                                           {"metadata.broker.list": broker_list})
    #     except:
    #         # raise ConnectionError("Kafka error: Connection refused: \
    #         #                     broker_list={} topic={}".format(broker_list, topic))
    #         logger.debug("Kafka error: Connection refused: broker_list={} topic={}".format(broker_list, topic))
    #         directKafkaStream = None
    #     time.sleep(10)

    parsed_lines = directKafkaStream.map(lambda v: json.loads(v[1]))

    # RDD handling
    parsed_lines.foreachRDD(process)

    return ssc


if __name__ == "__main__":
    time.sleep(20)
    if len(sys.argv) != 3:
        print("Usage: spark_job.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    print("--> Creating new context")
    if os.path.exists(outputPath):
        shutil.rmtree(outputPath)

    ssc = StreamingContext.getOrCreate(outputPath, lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
