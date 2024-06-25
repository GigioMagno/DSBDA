# Big Data & Analytics project: AirB&B
This project aims to analyze and discover Gentrification and possible trends related to this phenomenon.

## Gentrification sentiment
In the script ```Gentrification Sentiment``` the hypothesis is "Analyze if there's a change in the sentiment of the reviews according to the gentrification". $\rightarrow$ the script ```Gentrification Sentiment Spark``` is a distributed implementation of the hypothesis.

## Map reduce
Used to perform a join between two tables of the dataset. The tables are ```listings_details``` and ```reviews_details```. The scripts associated to this step are ```mapper.py``` and ```reduer.py```

## Spark Job
```sparkJob``` used to perform cleaning of all the dataset, drop columns and save on the distributed file system the output of the job

## Statistics
Used to extract preliminary hypothesis ```statistics```

## Spark Stream
```spark_receiver```, ```data_streamer```, ```data_generator``` used to capture data from a streaming of data and load them into hdfs

# Note
SINCE HADOOP AND THE SPARK STREAMING CONTEXT DON'T WORK ON THE VIRTUAL MACHINE, SOME CSVs FILES HAS BEEN USED TO "SIMULATE" THE HDFS ENVIRONMENT
