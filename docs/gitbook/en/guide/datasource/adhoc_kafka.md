# AdHoc Kafka

This is a datasource implementation for quick query in Kafka with Spark. 
You can control the parallelism of data fetching from kafka, and is not limited by 
the original size of kafka partitions.  It is useful especially when you just want 
to filter some data from kafka sometimes and it's definitely not a usual job. It saves you 
a lot of time since the traditional way is consuming kafka and write the data to HDFS/ES first.  