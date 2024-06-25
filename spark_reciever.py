#!/usr/bin/env python

import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row

def translate_to_df_and_save(rdd, num_of_columns, targetfile):
    print('FUNCTION CALLED')
    try:
        if not rdd.isEmpty():
            print('DATA INSIDE THE RDD')
            spark = SparkSession.builder.getOrCreate()
            print("Raw RDD data:", rdd.collect())
            row = create_rows(n_of_columns)
            df = rdd.map(row).toDF()
            df.show()
            df.write.text(f'hdfs://localhost:54310/airbnb_dataset/from_spark/{targetfile}')
        else:
            print('RDD EMPTY!')
    except Exception as e:
        print(e)

### This function returns the rows needed for the spark dataframe from
### the given number of fiels

def create_rows(n_of_columns):
    fields = {f"col_{i+1}": None for i in range(n_of_columns)}
    return Row(**fields)

def parse_csv_line(line):
    parts = line.split(',')
    fixed_parts = parts[:-1]
    last_field = ','.join(parts[len(fixed_parts):])
    fixed_parts.append(last_field)
    return fixed_parts

### This script is a mockup. We are not able to test it because spark does not properly run.

def main():

    is_calendar = lambda x: len(x) == 4
    is_review = lambda x: len(x) == 2
    is_listings = lambda x: len(x) == 16
    is_listing_details = lambda x: len(x) == 96
    is_reviews_details = lambda x: len(x) == 6



    sc = pyspark.SparkContext(appName = 'test1')
    ssc = StreamingContext(sc, 1)  # Un intervallo batch di 1 secondo
    host = "localhost"
    port = 7777
    socket_stream = ssc.socketTextStream(host, port)
    lines = socket_stream.window(20)# Considera i dati in una finestra
    lines.pprint()
    splitted = lines.map(parse_csv_line)
    splitted.pprint()

    ### Filtering the recieved window in to the possible cases. If a row does not satisfy the lambdas
    ### It gets discarded because only the content that satisfies the filters gets saved.

    calendar = splitted.filter(lambda line: is_calendar(line))
    review = splitted.filter(lambda line: is_review(line))
    listing = splitted.filter(lambda line: is_listings(line))
    listing_detail = splitted.filter(lambda line: is_listing_details(line))
    review_detail = splitted.filter(lambda line: is_reviews_details(line))

  #  calendar.foreachRDD(lambda rdd: translate_to_df_and_save(rdd, 4, "calendar1.txt"))
  #  review.foreachRDD(lambda rdd: translate_to_df_and_save(rdd, 2, "review1.txt"))
  #  listing.foreachRDD(lambda rdd: translate_to_df_and_save(rdd, 16, "listings.txt"))
  #  listing_detail.foreachRDD(lambda rdd: translate_to_df_and_save(rdd, 96, "listings_details.txt"))
  #  review_detail.foreachRDD(lambda rdd: translate_to_df_and_save(rdd, 6, "review_details.txt"))

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000)

if __name__ == '__main__':
    main()