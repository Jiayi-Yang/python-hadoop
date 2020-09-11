# Hive
(1) Distinguish HDFS(Hadoop Distributed File System)and Linux File System. Get familiar with HDFS commands. Practice following commands:
- List directories / files in HDFS

`hdfs dfs -ls`
- Copy files from LinuxFile System to HDFS

`hdfs dfs -put abc.txt test0830/abc.txt`
- Copy files from HDFS to Linux File System

```bash
hdfs dfs -get '/user/hive/warehouse/jiayiyang_db.db/categories//*parquet' ./
```

(2) In your hdfs home folder(/user/<your-id>), create a folder named banklist, and copy/data/banklist/banklist.csv to the bank list folder you just created

```bash
hdfs dfs -mkdir banklist
hdfs dfs -cp /data/banklist/banklist.csv ./banklist/
```

(3) In your database, create a Hive managed table using the CSV files. The CSV file has a header(first line) which gives the name of the columns. Column ‘CERT’ has type ‘int’, other columns have type ‘string’. After create the table, run some queries to verify the table is created correctly.

```sql
create table if not exists jiayiyang_db.banklists(
    `Bank Name` string,
    City string,
    ST string,
    CERT int,
    `Acquiring Institution` string,
    `Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
load data inpath '/user/jiayiyang/banklist' into table jiayiyang_db.banklists;
```

- desc formatted <yourtable>;
```
col_name,data_type,comment
# col_name            ,data_type           ,comment             
,NULL,NULL
bank name,string,from deserializer
city,string,from deserializer
st,string,from deserializer
cert,string,from deserializer
acquiring institution,string,from deserializer
closing date,string,from deserializer
,NULL,NULL
# Detailed Table Information,NULL,NULL
Database:           ,jiayiyang_db        ,NULL
OwnerType:          ,USER                ,NULL
Owner:              ,jiayiyang           ,NULL
CreateTime:         ,Fri Sep 11 21:39:24 UTC 2020,NULL
LastAccessTime:     ,UNKNOWN             ,NULL
Retention:          ,0                   ,NULL
Location:           ,hdfs://nameservice1/user/hive/warehouse/jiayiyang_db.db/banklists,NULL
Table Type:         ,MANAGED_TABLE       ,NULL
Table Parameters:,NULL,NULL
,numFiles            ,1                   
,numRows             ,0                   
,rawDataSize         ,0                   
,skip.header.line.count,1                   
,totalSize           ,41343               
,transient_lastDdlTime,1599860375          
,NULL,NULL
# Storage Information,NULL,NULL
SerDe Library:      ,org.apache.hadoop.hive.serde2.OpenCSVSerde,NULL
InputFormat:        ,org.apache.hadoop.mapred.TextInputFormat,NULL
OutputFormat:       ,org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat,NULL
Compressed:         ,No                  ,NULL
Num Buckets:        ,-1                  ,NULL
Bucket Columns:     ,[]                  ,NULL
Sort Columns:       ,[]                  ,NULL
Storage Desc Params:,NULL,NULL
,escapeChar          ,\\                  
,quoteChar           ,"\""                  "
,separatorChar       ,",                   "
,serialization.format,1                   


```
- select * from <yourtable> limit5;
```
banklists.bank name,banklists.city,banklists.st,banklists.cert,banklists.acquiring institution,banklists.closing date
The First State Bank,Barboursville,WV,14361,"MVB Bank, Inc.",3-Apr-20
Ericson State Bank,Ericson,NE,18265,Farmers and Merchants Bank,14-Feb-20
City National Bank of New Jersey,Newark,NJ,21111,Industrial Bank,1-Nov-19
Resolute Bank,Maumee,OH,58317,Buckeye State Bank,25-Oct-19
Louisa Community Bank,Louisa,KY,58112,Kentucky Farmers Bank Corporation,25-Oct-19

```
- select count(*) from <yourtable>;

`561`

Hint:Use org.apache.hadoop.hive.serde2.OpenCSVSerde. Study the banklist.csv file (hint:some columns have quoted text).Do some research using google to see which SERDEPROPERTIES you need to specify.

(4)In your database, create a Hive external table using the CSV files. After create the table,run some queries to verify the table is created correctly.
```bash
hdfs dfs -mkdir banklist_dbex
hdfs dfs -cp /data/banklist/banklist.csv ./banklist_dbex/
```
```sql
create external table if not exists jiayiyang_db.banklists_ex(
    `Bank Name` string,
    City string,
    ST string,
    CERT int,
    `Acquiring Institution` string,
    `Closing Date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '"',
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
location '/user/jiayiyang/banklist_dbex'
tblproperties ("skip.header.line.count"="1");
```
- desc formatted <yourtable>;
```
col_name,data_type,comment
# col_name            ,data_type           ,comment             
,NULL,NULL
bank name,string,from deserializer
city,string,from deserializer
st,string,from deserializer
cert,string,from deserializer
acquiring institution,string,from deserializer
closing date,string,from deserializer
,NULL,NULL
# Detailed Table Information,NULL,NULL
Database:           ,jiayiyang_db        ,NULL
OwnerType:          ,USER                ,NULL
Owner:              ,jiayiyang           ,NULL
CreateTime:         ,Fri Sep 11 21:52:43 UTC 2020,NULL
LastAccessTime:     ,UNKNOWN             ,NULL
Retention:          ,0                   ,NULL
Location:           ,hdfs://nameservice1/user/jiayiyang/banklist_dbex,NULL
Table Type:         ,EXTERNAL_TABLE      ,NULL
Table Parameters:,NULL,NULL
,EXTERNAL            ,TRUE                
,numFiles            ,1                   
,skip.header.line.count,1                   
,totalSize           ,41343               
,transient_lastDdlTime,1599861163          
,NULL,NULL
# Storage Information,NULL,NULL
SerDe Library:      ,org.apache.hadoop.hive.serde2.OpenCSVSerde,NULL
InputFormat:        ,org.apache.hadoop.mapred.TextInputFormat,NULL
OutputFormat:       ,org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat,NULL
Compressed:         ,No                  ,NULL
Num Buckets:        ,-1                  ,NULL
Bucket Columns:     ,[]                  ,NULL
Sort Columns:       ,[]                  ,NULL
Storage Desc Params:,NULL,NULL
,escapeChar          ,\\                  
,quoteChar           ,"\""                  "
,separatorChar       ,",                   "
,serialization.format,1                   

```
- select * from <yourtable> limit5;
```
banklists_ex.bank name,banklists_ex.city,banklists_ex.st,banklists_ex.cert,banklists_ex.acquiring institution,banklists_ex.closing date
The First State Bank,Barboursville,WV,14361,"MVB Bank, Inc.",3-Apr-20
Ericson State Bank,Ericson,NE,18265,Farmers and Merchants Bank,14-Feb-20
City National Bank of New Jersey,Newark,NJ,21111,Industrial Bank,1-Nov-19
Resolute Bank,Maumee,OH,58317,Buckeye State Bank,25-Oct-19
Louisa Community Bank,Louisa,KY,58112,Kentucky Farmers Bank Corporation,25-Oct-19

```
- select count(*) from <yourtable>;

`561`
- drop table <your table>; verify the data folder is not deleted by Hive.
the data folder still there
![external-delete](https://files.catbox.moe/h9cga8.png)


(5)Create a Hive table using AVRO file where you get from the SQOOP homework.
```bash
hdfs dfs -mkdir order_items_db
hdfs dfs -put order_items_files/order_items.avro order_items_db/
avro-tools getschema order_items_files/order_items.avro
```  
```sql
create external table if not exists jiayiyang_db.order_items(
    order_item_id int,
    order_item_order_id int,
    order_item_product_id int,
    order_item_quantity int,
    order_item_subtotal float,
    order_item_product_price float
)
stored as avro
location '/user/jiayiyang/order_items_db';
select * from jiayiyang_db.order_items limit 5;
```
![external-avro](https://files.catbox.moe/w73pdt.png)