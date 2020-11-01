## 1. Create a Kafka Topic
```bash
kafka-topics --bootstrap-server ip-172-31-91-232.ec2.internal:9092 --create --replication-factor 2 --partitions 2 --topic rsvp_jaye
```
## 2. Create Kafka Data Source
### Method 1: Console producer
```bash
curl https://stream.meetup.com/2/rsvps | kafka-console-producer -broker-list ip-172-31-91-232.ec2.internal:9092 --topic rsvp_jaye
```
### Method 2: Kafka producer and consumer through Python scripts
- see the rsvp repo/data_feed
### 3. Spark Streaming
#### 1: Save events in HDFS in text(json)format.  Use "kafka"source and "file"sink.  Set outputMode to "append".
```bash
cd /opt/cloudera/parcels/CDH/bin
./pyspark
spark.conf.set("spark.sql.shuffle.partitions",2)
```
```python
spark.conf.set("spark.sql.shuffle.partitions",2)
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092").option("subscribe", "rsvp_jaye").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
df.printSchema()
# select the value column and cast to string type
df1=df.select(df['value'].cast("string"))
df1.writeStream.trigger(processingTime="60 seconds").format("json").option("path", "/user/jiayiyang/rsvp/rsvp_sink/json").option("checkpointLocation", "/user/jiayiyang/rsvp/rsvp_checkpoint/checkpoint0").outputMode("append").start()
```
#### 2: Save events in HDFS in parquet format with schema.  Use "kafka"source and "file"sink.  Set outputMode to "append".
```python
spark.conf.set("spark.sql.shuffle.partitions",2)
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092").option("subscribe", "rsvp_jaye").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
df1 = df.select(df['value'].cast("string"))
# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema
from pyspark.sql.functions import *
# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"))
df2.printSchema()
# select all columns from the converted column
# need to change the checkpoint path each time
df3 = df2.select("json.*")
df3.writeStream.trigger(processingTime="60 seconds").format("parquet").option("path", "/user/jiayiyang/rsvp/rsvp_sink/parquet").option("checkpointLocation", "/user/jiayiyang/rsvp/rsvp_checkpoint/checkpoint1").outputMode("append").start()
```
- Output Schema
```
>>> df3.printSchema()
root
 |-- event: struct (nullable = true)
 |    |-- event_id: string (nullable = true)
 |    |-- event_name: string (nullable = true)
 |    |-- event_url: string (nullable = true)
 |    |-- time: long (nullable = true)
 |-- group: struct (nullable = true)
 |    |-- group_city: string (nullable = true)
 |    |-- group_country: string (nullable = true)
 |    |-- group_id: long (nullable = true)
 |    |-- group_lat: double (nullable = true)
 |    |-- group_lon: double (nullable = true)
 |    |-- group_name: string (nullable = true)
 |    |-- group_state: string (nullable = true)
 |    |-- group_topics: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- topic_name: string (nullable = true)
 |    |    |    |-- urlkey: string (nullable = true)
 |    |-- group_urlname: string (nullable = true)
 |-- guests: long (nullable = true)
 |-- member: struct (nullable = true)
 |    |-- member_id: long (nullable = true)
 |    |-- member_name: string (nullable = true)
 |    |-- photo: string (nullable = true)
 |-- mtime: long (nullable = true)
 |-- response: string (nullable = true)
 |-- rsvp_id: long (nullable = true)
 |-- venue: struct (nullable = true)
 |    |-- lat: double (nullable = true)
 |    |-- lon: double (nullable = true)
 |    |-- venue_id: long (nullable = true)
 |    |-- venue_name: string (nullable = true)
 |-- visibility: string (nullable = true)
```
- Check result file
```python
path = '/user/jiayiyang/rsvp/rsvp_sink/parquet/*.parquet'
temp = spark.read.parquet(path)
```
```bash
>>> temp.show(5)
+--------------------+--------------------+------+--------------------+-------------+--------+----------+--------------------+----------+
|               event|               group|guests|              member|        mtime|response|   rsvp_id|               venue|visibility|
+--------------------+--------------------+------+--------------------+-------------+--------+----------+--------------------+----------+
|[273972577, 🏸 BA...|[Osaka, jp, 18694...|     0|[204288286, Takuy...|1604013374628|     yes|1854600798|[34.68086, 135.48...|    public|
|[tbcggrybcpbhb, C...|[Johannesburg, za...|     0|[268539017, Amadi...|1604013377914|     yes|1854600800|[-8.521147, 179.1...|    public|
|[269381285, Beer ...|[Oakland, us, 191...|     0|[196856176, Ceci ...|1604013378240|      no|1845866149|[37.796753, -122....|    public|
|[xntpxrybcpblb, F...|[Auckland, nz, 34...|     0|[23429851, Daisy,...|1604013380838|     yes|1854600802|[-36.862526, 174....|    public|
|[274259413, Eisen...|[Washington, us, ...|     0|   [114190022, MCJ,]|1604013385315|     yes|1854600804|[-8.521147, 179.1...|    public|
+--------------------+--------------------+------+--------------------+-------------+--------+----------+--------------------+----------+```
```
#### 3: Show how many events are received, display in a 2-minute tumbling window.  Show result at 1-minute interval.  Use "kafka"source and "console"sink.Set outputMode to "complete".
```python
spark.conf.set("spark.sql.shuffle.partitions",2)
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092").option("subscribe", "rsvp_jaye").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
df1 = df.select(df['value'].cast("string"))
# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema
from pyspark.sql.functions import *
# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"))
df2.printSchema()
# select all columns from the converted column
df3 = df2.select("json.*")
# convert time from unix millisecond to timestamp
df4 = df3.withColumn("event_dt", to_timestamp(df3["mtime"]/1000)).select("event_dt")
# create the tumbling window
df4.groupBy(window(col("event_dt"), "2 minutes")).count().writeStream.trigger(processingTime="60 seconds").queryName("events_per_window1").format("console").outputMode("complete").option("truncate", "false").start()
```
```bash
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|[2020-10-28 12:00:00, 2020-10-28 12:02:00]|2    |
|[2020-10-29 23:50:00, 2020-10-29 23:52:00]|104  |
|[2020-10-29 22:16:00, 2020-10-29 22:18:00]|1    |
|[2020-10-29 11:52:00, 2020-10-29 11:54:00]|1    |
|[2020-10-27 22:16:00, 2020-10-27 22:18:00]|1    |
|[2020-10-30 00:14:00, 2020-10-30 00:16:00]|76   |
|[2020-10-30 00:18:00, 2020-10-30 00:20:00]|27   |
|[2020-10-27 15:32:00, 2020-10-27 15:34:00]|1    |
|[2020-10-30 23:12:00, 2020-10-30 23:14:00]|88   |
|[2020-10-03 01:28:00, 2020-10-03 01:30:00]|1    |
|[2020-10-29 23:16:00, 2020-10-29 23:18:00]|23   |
|[2020-10-30 23:14:00, 2020-10-30 23:16:00]|107  |
|[2020-09-19 01:46:00, 2020-09-19 01:48:00]|1    |
|[2020-10-30 23:24:00, 2020-10-30 23:26:00]|93   |
|[2020-10-30 00:02:00, 2020-10-30 00:04:00]|13   |
|[2020-10-30 23:00:00, 2020-10-30 23:02:00]|1    |
|[2020-10-30 23:16:00, 2020-10-30 23:18:00]|49   |
|[2020-10-30 23:28:00, 2020-10-30 23:30:00]|65   |
|[2020-10-29 23:36:00, 2020-10-29 23:38:00]|29   |
|[2020-10-29 23:54:00, 2020-10-29 23:56:00]|19   |
+------------------------------------------+-----+
only showing top 20 rows
```
- need to save the tumbling window in memory for sql to get data and sort by window time
```python
df4.groupBy(window(col("event_dt"), "2 minutes")).count().writeStream.trigger(processingTime="60 seconds").queryName("events_per_window2").format("memory").outputMode("complete").option("truncate", "false").start()
spark.sql('select * from events_per_window2 order by window').show(20,False)
```
```bash
+------------------------------------------+-----+
|window                                    |count|
+------------------------------------------+-----+
|[2020-09-19 01:46:00, 2020-09-19 01:48:00]|1    |
|[2020-10-03 01:28:00, 2020-10-03 01:30:00]|1    |
|[2020-10-24 08:26:00, 2020-10-24 08:28:00]|1    |
|[2020-10-25 01:24:00, 2020-10-25 01:26:00]|1    |
|[2020-10-26 02:24:00, 2020-10-26 02:26:00]|1    |
|[2020-10-26 14:16:00, 2020-10-26 14:18:00]|1    |
|[2020-10-26 20:38:00, 2020-10-26 20:40:00]|1    |
|[2020-10-27 15:32:00, 2020-10-27 15:34:00]|1    |
|[2020-10-27 22:16:00, 2020-10-27 22:18:00]|1    |
|[2020-10-28 12:00:00, 2020-10-28 12:02:00]|2    |
|[2020-10-29 11:52:00, 2020-10-29 11:54:00]|1    |
|[2020-10-29 20:42:00, 2020-10-29 20:44:00]|1    |
|[2020-10-29 22:16:00, 2020-10-29 22:18:00]|1    |
|[2020-10-29 22:28:00, 2020-10-29 22:30:00]|1    |
|[2020-10-29 23:08:00, 2020-10-29 23:10:00]|1    |
|[2020-10-29 23:16:00, 2020-10-29 23:18:00]|23   |
|[2020-10-29 23:36:00, 2020-10-29 23:38:00]|29   |
|[2020-10-29 23:38:00, 2020-10-29 23:40:00]|72   |
|[2020-10-29 23:40:00, 2020-10-29 23:42:00]|50   |
|[2020-10-29 23:44:00, 2020-10-29 23:46:00]|1    |
+------------------------------------------+-----+
```
#### 4：Show how many events are received for each country, display it in a sliding window(set windowDuration to 3 minutes and slideDuration to 1 minutes).  Show result at 1-minute interval.  Use "kafka" source and "console" sink.
- use df3 continued from question 3
```python
df5 = df3.withColumn("event_dt", to_timestamp(df3["mtime"]/1000)).select("event_dt","group.group_country")
df5.groupBy(window(col("event_dt"), "3 minutes", "60 seconds"),col("group_country"),).count().writeStream.trigger(processingTime="60 seconds").queryName("events_per_window4").format("console").outputMode("complete").option("truncate", "false").start()
```
```bash
Batch: 0
-------------------------------------------
+------------------------------------------+-------------+-----+
|window                                    |group_country|count|
+------------------------------------------+-------------+-----+
|[2020-10-30 23:22:00, 2020-10-30 23:25:00]|ar           |1    |
|[2020-10-29 23:14:00, 2020-10-29 23:17:00]|gb           |1    |
|[2020-10-30 00:24:00, 2020-10-30 00:27:00]|lk           |1    |
|[2020-10-29 23:51:00, 2020-10-29 23:54:00]|br           |1    |
|[2020-10-29 23:49:00, 2020-10-29 23:52:00]|cl           |1    |
|[2020-10-29 23:40:00, 2020-10-29 23:43:00]|gb           |4    |
|[2020-10-29 23:51:00, 2020-10-29 23:54:00]|us           |83   |
|[2020-10-29 23:37:00, 2020-10-29 23:40:00]|au           |10   |
|[2020-10-30 23:10:00, 2020-10-30 23:13:00]|jp           |1    |
|[2020-10-30 00:01:00, 2020-10-30 00:04:00]|ch           |1    |
|[2020-10-29 23:14:00, 2020-10-29 23:17:00]|sa           |1    |
|[2020-10-29 23:38:00, 2020-10-29 23:41:00]|hk           |4    |
|[2020-10-30 00:00:00, 2020-10-30 00:03:00]|bo           |1    |
|[2020-10-30 00:18:00, 2020-10-30 00:21:00]|au           |2    |
|[2020-10-30 23:15:00, 2020-10-30 23:18:00]|gb           |6    |
|[2020-10-30 23:20:00, 2020-10-30 23:23:00]|tr           |1    |
|[2020-10-30 23:21:00, 2020-10-30 23:24:00]|pl           |1    |
|[2020-10-29 23:40:00, 2020-10-29 23:43:00]|jp           |1    |
|[2020-10-24 08:26:00, 2020-10-24 08:29:00]|au           |1    |
|[2020-10-30 23:28:00, 2020-10-30 23:31:00]|us           |41   |
+------------------------------------------+-------------+-----+
only showing top 20 rows
```
- need to save the tumbling window in memory for sql to get data and sort by window time
```python
df5.groupBy(window(col("event_dt"), "3 minutes", "60 seconds"),col("group_country"),).count().writeStream.trigger(processingTime="60 seconds").queryName("events_per_window5").format("memory").outputMode("complete").option("truncate", "false").start()
spark.sql('select * from events_per_window5 order by window').show(20,False)
```
```bash
+------------------------------------------+-------------+-----+
|window                                    |group_country|count|
+------------------------------------------+-------------+-----+
|[2020-09-19 01:44:00, 2020-09-19 01:47:00]|us           |1    |
|[2020-09-19 01:45:00, 2020-09-19 01:48:00]|us           |1    |
|[2020-09-19 01:46:00, 2020-09-19 01:49:00]|us           |1    |
|[2020-10-03 01:26:00, 2020-10-03 01:29:00]|us           |1    |
|[2020-10-03 01:27:00, 2020-10-03 01:30:00]|us           |1    |
|[2020-10-03 01:28:00, 2020-10-03 01:31:00]|us           |1    |
|[2020-10-24 08:25:00, 2020-10-24 08:28:00]|au           |1    |
|[2020-10-24 08:26:00, 2020-10-24 08:29:00]|au           |1    |
|[2020-10-24 08:27:00, 2020-10-24 08:30:00]|au           |1    |
|[2020-10-25 01:23:00, 2020-10-25 01:26:00]|us           |1    |
|[2020-10-25 01:24:00, 2020-10-25 01:27:00]|us           |1    |
|[2020-10-25 01:25:00, 2020-10-25 01:28:00]|us           |1    |
|[2020-10-26 02:23:00, 2020-10-26 02:26:00]|au           |1    |
|[2020-10-26 02:24:00, 2020-10-26 02:27:00]|au           |1    |
|[2020-10-26 02:25:00, 2020-10-26 02:28:00]|au           |1    |
|[2020-10-26 14:14:00, 2020-10-26 14:17:00]|us           |1    |
|[2020-10-26 14:15:00, 2020-10-26 14:18:00]|us           |1    |
|[2020-10-26 14:16:00, 2020-10-26 14:19:00]|us           |1    |
|[2020-10-26 20:37:00, 2020-10-26 20:40:00]|us           |1    |
|[2020-10-26 20:38:00, 2020-10-26 20:41:00]|us           |1    |
+------------------------------------------+-------------+-----+
only showing top 20 rows
```
#### 5： Use impala to create a KUDU table. Do dataframe transformation to extract information and write to the KUDU table. Use "kafka"source and "kudu"sink. 
- Impala
```sql
create table if not exists rsvp_db.rsvp_kudu_jaye
(
  rsvp_id bigint primary key,
  member_id bigint,
  member_name string,
  group_id bigint,
  group_name string,
  group_city string,
  group_country string,
  event_name string,
  event_time bigint
)
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;
```
- Pyspark Shell
```bash
./pyspark --packages "org.apache.kudu:kudu-spark2_2.11:1.10.0-cdh6.3.2" --repositories "https://repository.cloudera.com/artifactory/cloudera-repos"
```
```python
spark.conf.set("spark.sql.shuffle.partitions",2)
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092").option("subscribe", "rsvp_jaye").option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()
df1 = df.select(df['value'].cast("string"))
# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema
from pyspark.sql.functions import *
# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"))
df2.printSchema()
# select all columns from the converted column
df3 = df2.select("json.*")
# convert time from unix millisecond to timestamp
df6 = df3.withColumn("event_time", df3["event.time"]).select("rsvp_id","member.member_id","member.member_name","group.group_id","group.group_name","group.group_city","group.group_country","event.event_name","event_time")
df6.printSchema()


```
- check schema
```
root
 |-- rsvp_id: long (nullable = true)
 |-- member_id: long (nullable = true)
 |-- member_name: string (nullable = true)
 |-- group_id: long (nullable = true)
 |-- group_name: string (nullable = true)
 |-- group_city: string (nullable = true)
 |-- group_country: string (nullable = true)
 |-- event_name: string (nullable = true)
 |-- event_time: long (nullable = true)
```
```python
# java.lang.IllegalArgumentException: Can't set primary key column 'rsvp_id' to null
# find rows missing data
df6.writeStream.format("console").start()
# filter out missing data
df7 = df6.where(col("rsvp_id").isNotNull())
# write to kudu master
df7.writeStream.trigger(processingTime="60 seconds").format("kudu").option("kudu.master", "ip-172-31-89-172.ec2.internal:7051").option("kudu.table", "impala::rsvp_db.rsvp_kudu_jaye").option("kudu.operation", "upsert").option("checkpointLocation", "/user/jiayiyang/rsvp/kudu_checkpoint/checkpoint0").outputMode("append").start()
```
- check from Impala to make sure data are correct

![kudu-impala](https://raw.githubusercontent.com/Jiayi-Yang/Imagebed/master/img/kudu.png)
## 6. Use YARN web console to monitor the job.
![kudu-impala](https://raw.githubusercontent.com/Jiayi-Yang/Imagebed/master/img/yarn.png)

