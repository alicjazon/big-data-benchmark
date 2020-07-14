#!/bin/bash

### SPARK PI LOCAL ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--class "SparkPi" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar 5 2000000

# bash /usr/local/Cellar/apache-spark/2.3.2/libexec/sbin/start-history-server.sh

export HADOOP_CONF_DIR=/usr/local/Cellar/apache-spark/2.3.2/libexec/etc/hadoop
export YARN_CONF_DIR=/usr/local/Cellar/apache-spark/2.3.2/libexec/etc/hadoop

### SPARK PI HADOOP ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--master yarn --deploy-mode cluster \
--class "SparkPi" /Users/Ala/Desktop/Programy-magisterka/sparkwordcount/target/scala-2.11/sparkwordcount_2.11-0.1.jar 2 2000000

### CASSANDRA TEST ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.cassandra.connection.host=127.0.0.1 \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--class "CassandraTest" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar 

### WORD COUNT ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--class "WordCount" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar 
## set number of executors
 --num-executors 1

### CASSANDRA WORD COUNT ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.cassandra.connection.host=127.0.0.1 \
--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
--class "CassandraWordCount" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar 

### WORD COUNT HDFS ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--master yarn --deploy-mode client \
--class "WordCountHdfs" target/scala-2.11/sparkwordcount_2.11-0.1.jar

### WORD COUNT HDFS HBASE ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit --conf \
spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events --master yarn \
--deploy-mode client --class "WordCountHbase" --jars \
/Users/Ala/Desktop/Programy-magisterka/sparkwordcount/target/scala-2.11/sparkwordcount-assembly-0.1-deps.jar \
/Users/Ala/Desktop/Programy-magisterka/sparkwordcount/target/scala-2.11/sparkwordcount_2.11-0.1.jar

### DATA BREACH WRITER ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--class "DataBreachWriter" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar

### DATA BREACH WRITER CASSANDRA ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit \
--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.cassandra.connection.host=127.0.0.1 \
--num-executors 2 --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
--class "DataBreachWriterCassandra" --master local[*] target/scala-2.11/sparkwordcount_2.11-0.1.jar

### DATA BREACH WRITER HDFS ###
/usr/local/Cellar/apache-spark/2.3.2/libexec/bin/spark-submit --conf \
spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events --master yarn \
--deploy-mode cluster --class "DataBreachWriterHdfs" --jars \
/Users/Ala/Desktop/Programy-magisterka/sparkwordcount/target/scala-2.11/sparkwordcount-assembly-0.1-deps.jar \
/Users/Ala/Desktop/Programy-magisterka/sparkwordcount/target/scala-2.11/sparkwordcount_2.11-0.1.jar

### HIVE
CREATE EXTERNAL TABLE hivehbasetable(key INT, word STRING, value INT) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, word:word, value:value") TBLPROPERTIES("hbase.table.name" = "wordcount");