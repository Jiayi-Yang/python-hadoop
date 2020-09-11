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
    Closing Date
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count"="1");
load data inpath '/user/jiayiyang/banklist' into table jiayiyang_db.banklists;
```

- desc formatted <yourtable>;
```
col_name	data_type	comment	
# col_name            	data_type           	comment             	
	NULL	NULL	
bank name	string		
city	string		
st	string		
cert	int		
acquiring institution	string		
closing	date		
	NULL	NULL	
# Detailed Table Information	NULL	NULL	
Database:           	jiayiyang_db        	NULL	
OwnerType:          	USER                	NULL	
Owner:              	jiayiyang           	NULL	
CreateTime:         	Fri Sep 11 01:54:12 UTC 2020	NULL	
LastAccessTime:     	UNKNOWN             	NULL	
Retention:          	0                   	NULL	
Location:           	hdfs://nameservice1/user/hive/warehouse/jiayiyang_db.db/banklists	NULL	
Table Type:         	MANAGED_TABLE       	NULL	
Table Parameters:	NULL	NULL	
	numFiles            	1                   	
	numRows             	0                   	
	rawDataSize         	0                   	
	skip.header.line.count	1                   	
	totalSize           	41343               	
	transient_lastDdlTime	1599789264          	
	NULL	NULL	
# Storage Information	NULL	NULL	
SerDe Library:      	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	NULL	
InputFormat:        	org.apache.hadoop.mapred.TextInputFormat	NULL	
OutputFormat:       	org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat	NULL	
Compressed:         	No                  	NULL	
Num Buckets:        	-1                  	NULL	
Bucket Columns:     	[]                  	NULL	
Sort Columns:       	[]                  	NULL	
Storage Desc Params:	NULL	NULL	
	field.delim         	,                   	
	serialization.format	,                   	
```
- select * from <yourtable> limit5;

- select count(*) from <yourtable>;

`561`

Hint:Use org.apache.hadoop.hive.serde2.OpenCSVSerde. Study the banklist.csv file (hint:some columns have quoted text).Do some research using google to see which SERDEPROPERTIES you need to specify.

(4)In your database, create a Hive external table using the CSV files. After create the table,run some queries to verify the table is created correctly.
- desc formatted <yourtable>;
- select * from <yourtable> limit5;
- select count(*) from <yourtable>;
- drop table <your table>; verify the data folder is not deleted by Hive.



(5)Create a Hive table using AVRO file where you get from the SQOOP homework.
    