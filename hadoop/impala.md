## 1.Banklist dataset
Write queries on banklist table:
- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
Impala doesn't work with original load with "OpenCSVSerde", have to create a new db with correct datatype.
```sql
// Hive
drop table banklists2;
create table banklists2(
    bank_name string,
    city string,
    st string,
    cert string,
    acquiring_institution string,
    closing_date timestamp
)
stored as parquet;

insert into table jiayiyang_db.banklists2
select `bank name`,city,st,cert,`acquiring institution`, 
from_unixtime(unix_timestamp(`closing date`,'dd-MMM-yy'),'yyyy-MM-dd')
from banklists;

// Impala
REFRESH banklists2; //Must have refresh here, because the table is created by Hive
SELECT st, count(*) AS num_by_state
FROM jiayiyang_db.banklists2
GROUP BY st
ORDER BY num_by_state DESC
LIMIT 5;
```
```
st	num_by_state	
GA	93	
FL	75	
IL	69	
CA	41	
MN	23	
```
- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```sql
SELECT closing_year, count(*) as num_closed
FROM(
    SELECT *, year(closing_date) as closing_year
    FROM jiayiyang_db.banklists2) a
GROUP BY closing_year;
```
```
closing_year	num_closed	
2000		2	
2001		4	
2002		11	
2003		3	
2004		4	
2007		3	
2008		25	
2009		140	
2010		157	
2011		92	
2012		51	
2013		24	
2014		18	
2015		8	
2016		5	
2017		8	
2019		4	
2020		2	
```

## 2.Chicago crime dataset:

https://data.cityofchicago.org/Public-Safety/Crimes-2020/qzdf-xmn8Check 
hive database named “chicago”, there is one table named:
crime_parquet: include data from 2001-present
- (1)In your own database, create a partitioned table(partitioned by year)to store data, store in parquet format.Name the table “crime_parquet_16_20”;
```sql
create table jiayiyang_db.crime_parquet_16_20 (
   id bigint,
   case_number string,
   `date` bigint,
   block  string,
   IUCR   string,
   primary_type   string,
   description    string,
   loc_desc   string,
   arrest     boolean,
   domestic   boolean,
   beat       string,
   district   string,
   ward       int,
   community_area string,
   FBI_code       string,
   x_coordinate   int,
   y_coordinate   int,
   updated_on     string,
   latitude       float,
   longitude      float,
   loc            string
)
partitioned by(yr int)
stored as parquet;
describe jiayiyang_db.crime_parquet_16_20;
```

- (2)Import 2016 to 2020 data into the partitioned table from table chicago.parquet.
- Dynamic Partition
