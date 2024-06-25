#!/usr/bin/env python

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row


def process_rdd(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("WriteCSVToHDFS").getOrCreate()
        row_rdd = rdd.map(lambda w: Row(word=w))
        df = spark.createDataFrame(row_rdd)
        hdfs_path = "hdfs://localhost:8020/user/ubuntu/dataset/" 
        df.write.csv(hdfs_path, mode="append", header=False)


def main():
    sc = SparkContext()
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 1)

    host = "localhost"
    port = 7778
    socket_stream = ssc.socketTextStream(host, port)
    lines = socket_stream.window(20)  # Considera i dati in una finestra di 20 secondi
    lines.pprint()
    words = lines.flatMap(lambda line: line.split(",")).map(lambda word: word.lower())

    words.foreachRDD(process_rdd)

    ssc.start()  # Avvia il calcolo
    ssc.awaitTermination()  # Aspetta che lo streaming termini


if __name__ == '__main__':
    main()
