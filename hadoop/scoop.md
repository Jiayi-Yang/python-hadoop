# SCOOP 
(1)Get familiar with HUE file browser HUE URL: https://hue.ascendingdc.com/hue

(2)In hdfs, create a folder named retail_db under your home folder (/user/<your-id>), and 3 sub-folder named parquet, avro and text
```bash
hdfs -dfs -mkdir /user/jiayiyang/retail_db/
hdfs -dfs -mkdir /user/jiayiyang/retail_db/parquet
hdfs -dfs -mkdir /user/jiayiyang/retail_db/avro
hdfs -dfs -mkdir /user/jiayiyang/retail_db/text
```

(3)Import all tables from MariaDB retail_db to hdfs, in parquet format, save in hdfs folder  /user/<your_id>/retail_db/parquet, each table is stored in a sub-folder, for example, products table should be imported to /user/<your-id>/retail_db/parquet/products/
```bash
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--compression-codec=snappy \
--as-parquetfile \
--warehouse-dir /user/jiayiyang/retail_db/parquet
```
(4)Import all tables from MariaDB retail_db to hdfs, in text format, use ‘#’ as field delimiter.  Save in hdfs folder  /user/<your_id>/retail_db/text, each table is stored in a sub-folder, for example, products table should be imported to /user/<your_id>/retail_db/text/products/
```bash
sqoop import-all-tables \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--fields-terminated-by '|' \
--warehouse-dir /user/jiayiyang/retail_db/text
```
(5)Import table order_items to hdfs, in avro format, save in hdfs folder /user/<your_id>/retail_db/avro
- First run with one map task, then with two map tasks, compare the console output. List the sqoop behavior difference between one map task and two map tasks.
```bash
sqoop import \
-m 1 \
--connect jdbc:mysql://database.ascendingdc.com:3306/retail_db \
--username=student \
--password=1234abcd \
--table order_items \
--compression-codec=snappy \
--as-avrodatafile \
--target-dir /user/jiayiyang/retail_db/avro
```
- 1 map task
```
20/09/09 23:06:55 INFO mapreduce.JobSubmitter: number of splits:1
20/09/09 23:07:02 INFO mapreduce.Job:  map 0% reduce 0%
20/09/09 23:07:08 INFO mapreduce.Job:  map 100% reduce 0%
```
- 2 map tasks
```
20/09/09 23:13:21 INFO db.IntegerSplitter: Split size: 86098; Num splits: 2 from: 1 to: 172198
20/09/09 23:13:21 INFO mapreduce.JobSubmitter: number of splits:2
20/09/09 23:13:28 INFO mapreduce.Job:  map 0% reduce 0%
20/09/09 23:13:35 INFO mapreduce.Job:  map 50% reduce 0%
20/09/09 23:13:36 INFO mapreduce.Job:  map 100% reduce 0%
```
- scoop diff
![scoop-2mappers](https://files.catbox.moe/def98t.png)

(6)In edge node, in your home folder, Create a folder named “order_items_files” in Linux file system.

`mkdir order_items_files`

(7)Copy order_items table files generated in step 3, 4, 5 from HDFS to Linux file system, name them as     
- order_items.parquet, 
```bash
hdfs dfs -get retail_db/parquet/order_items/*.parquet order_items_files/order_items.parquet
```
- order_items.avro, 
```bash
hdfs dfs -get retail_db/avro/part-m-00000.avro order_items_files/order_items.avro
```
- order_items.txt 
```bash
hdfs dfs -get retail_db/text/order_items/part-m-00000 order_items_files/order_items.txt
```
    
(8)Use parquet-tools to show following information from order_items.parquet
- schema 
```bash
parquet-tools schema order_items_files/order_items.parquet
```
```sql
message order_items {
  optional int32 order_item_id;
  optional int32 order_item_order_id;
  optional int32 order_item_product_id;
  optional int32 order_item_quantity;
  optional float order_item_subtotal;
  optional float order_item_product_price;
}
```
- metadata
```bash
parquet-tools meta order_items_files/order_items.parquet
```
```
file:                     file:/home/jiayiyang/order_items_files/order_items.parquet
creator:                  parquet-mr version 1.9.0-cdh6.3.2 (build ${buildNumber})
extra:                    parquet.avro.schema = {"type":"record","name":"order_items","doc":"Sqoop import of order_items","fields":[{"name":"order_item_id","type":["null","int"],"default":null,"columnName":"order_item_id","sqlType":"4"},{"name":"order_item_order_id","type":["null","int"],"default":null,"columnName":"order_item_order_id","sqlType":"4"},{"name":"order_item_product_id","type":["null","int"],"default":null,"columnName":"order_item_product_id","sqlType":"4"},{"name":"order_item_quantity","type":["null","int"],"default":null,"columnName":"order_item_quantity","sqlType":"-6"},{"name":"order_item_subtotal","type":["null","float"],"default":null,"columnName":"order_item_subtotal","sqlType":"7"},{"name":"order_item_product_price","type":["null","float"],"default":null,"columnName":"order_item_product_price","sqlType":"7"}],"tableName":"order_items"}
extra:                    writer.model.name = avro

file schema:              order_items
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
order_item_id:            OPTIONAL INT32 R:0 D:1
order_item_order_id:      OPTIONAL INT32 R:0 D:1
order_item_product_id:    OPTIONAL INT32 R:0 D:1
order_item_quantity:      OPTIONAL INT32 R:0 D:1
order_item_subtotal:      OPTIONAL FLOAT R:0 D:1
order_item_product_price: OPTIONAL FLOAT R:0 D:1

row group 1:              RC:172198 TS:1782884 OFFSET:4
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
order_item_id:             INT32 SNAPPY DO:0 FPO:4 SZ:688882/688851/1.00 VC:172198 ENC:PLAIN,RLE,BIT_PACKED ST:[min: 1, max: 172198, num_nulls: 0]
order_item_order_id:       INT32 SNAPPY DO:0 FPO:688886 SZ:437688/574545/1.31 VC:172198 ENC:PLAIN_DICTIONARY,RLE,BIT_PACKED ST:[min: 1, max: 68883, num_nulls: 0]
order_item_product_id:     INT32 SNAPPY DO:0 FPO:1126574 SZ:151510/151493/1.00 VC:172198 ENC:PLAIN_DICTIONARY,RLE,BIT_PACKED ST:[min: 19, max: 1073, num_nulls: 0]
order_item_quantity:       INT32 SNAPPY DO:0 FPO:1278084 SZ:64890/64882/1.00 VC:172198 ENC:PLAIN_DICTIONARY,RLE,BIT_PACKED ST:[min: 1, max: 5, num_nulls: 0]
order_item_subtotal:       FLOAT SNAPPY DO:0 FPO:1342974 SZ:173333/173318/1.00 VC:172198 ENC:PLAIN_DICTIONARY,RLE,BIT_PACKED ST:[min: 9.99, max: 1999.99, num_nulls: 0]
order_item_product_price:  FLOAT SNAPPY DO:0 FPO:1516307 SZ:129808/129795/1.00 VC:172198 ENC:PLAIN_DICTIONARY,RLE,BIT_PACKED ST:[min: 9.99, max: 1999.99, num_nulls: 0]
```
- rowcount
```bash
parquet-tools rowcount order_items_files/order_items.parquet
```
```
Total RowCount: 172198
```
- first 5 records
```bash
parquet-tools head order_items_files/order_items.parquet
```
```
order_item_id = 1
order_item_order_id = 1
order_item_product_id = 957
order_item_quantity = 1
order_item_subtotal = 299.98
order_item_product_price = 299.98

order_item_id = 2
order_item_order_id = 2
order_item_product_id = 1073
order_item_quantity = 1
order_item_subtotal = 199.99
order_item_product_price = 199.99

order_item_id = 3
order_item_order_id = 2
order_item_product_id = 502
order_item_quantity = 5
order_item_subtotal = 250.0
order_item_product_price = 50.0

order_item_id = 4
order_item_order_id = 2
order_item_product_id = 403
order_item_quantity = 1
order_item_subtotal = 129.99
order_item_product_price = 129.99

order_item_id = 5
order_item_order_id = 4
order_item_product_id = 897
order_item_quantity = 2
order_item_subtotal = 49.98
order_item_product_price = 24.99
```
    
(9) Examine text file order_items.txt, and calculate rowcount, compare it with the rowcount in step (8)
```bash
wc -l  order_items_files/order_items.txt
172198 order_items_files/order_items.txt
```
(10) Use avro-tools to show following information from order_items.avro
- schema
```bash
avro-tools getschema order_items_files/order_items.avro
```
```
{
  "type" : "record",
  "name" : "order_items",
  "doc" : "Sqoop import of order_items",
  "fields" : [ {
    "name" : "order_item_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_item_id",
    "sqlType" : "4"
  }, {
    "name" : "order_item_order_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_item_order_id",
    "sqlType" : "4"
  }, {
    "name" : "order_item_product_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_item_product_id",
    "sqlType" : "4"
  }, {
    "name" : "order_item_quantity",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_item_quantity",
    "sqlType" : "-6"
  }, {
    "name" : "order_item_subtotal",
    "type" : [ "null", "float" ],
    "default" : null,
    "columnName" : "order_item_subtotal",
    "sqlType" : "7"
  }, {
    "name" : "order_item_product_price",
    "type" : [ "null", "float" ],
    "default" : null,
    "columnName" : "order_item_product_price",
    "sqlType" : "7"
  } ],
  "tableName" : "order_items"
}
```
- metadata
```bash
avro-tools getmeta order_items_files/order_items.avro
```
```
avro.schema	{"type":"record","name":"order_items","doc":"Sqoop import of order_items","fields":[{"name":"order_item_id","type":["null","int"],"default":null,"columnName":"order_item_id","sqlType":"4"},{"name":"order_item_order_id","type":["null","int"],"default":null,"columnName":"order_item_order_id","sqlType":"4"},{"name":"order_item_product_id","type":["null","int"],"default":null,"columnName":"order_item_product_id","sqlType":"4"},{"name":"order_item_quantity","type":["null","int"],"default":null,"columnName":"order_item_quantity","sqlType":"-6"},{"name":"order_item_subtotal","type":["null","float"],"default":null,"columnName":"order_item_subtotal","sqlType":"7"},{"name":"order_item_product_price","type":["null","float"],"default":null,"columnName":"order_item_product_price","sqlType":"7"}],"tableName":"order_items"}
avro.codec	snappy
```
- rowcount
```
[jiayiyang@ip-172-31-92-98 ~]$ wc  order_items_files/order_items.json
  172198   172198 37609906 order_items_files/order_items.json
```
- convert to json files
```bash
avro-tools tojson order_items_files/order_items.avro > order_items_files/order_items.json
```