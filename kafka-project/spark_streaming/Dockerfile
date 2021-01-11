#FROM python:3
#
#RUN pip install --upgrade pip && \
#    pip install numpy &&\
#    pip install pyspark==2.4.6 &&\
#    pip install --no-cache-dir org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 org.postgresql:postgresql:9.4.1207
FROM bitnami/spark:2.4.6
ADD spark_job.py /spark_streaming/
ADD ./sql/university_score.sql /spark_streaming/sql/
WORKDIR /spark_streaming/