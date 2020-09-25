## 1.Banklist dataset
Write queries on banklist table:
- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
```sql
SELECT st, count(*) AS num_by_state
FROM jiayiyang_db.banklists
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
    SELECT *, concat('20',substring_index(`closing date`,'-',-1)) as closing_year
    FROM jiayiyang_db.banklists) a
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
```
col_name	data_type	comment	
id	bigint		
case_number	string		
date	bigint		
block	string		
iucr	string		
primary_type	string		
description	string		
loc_desc	string		
arrest	boolean		
domestic	boolean		
beat	string		
district	string		
ward	int		
community_area	string		
fbi_code	string		
x_coordinate	int		
y_coordinate	int		
updated_on	string		
latitude	float		
longitude	float		
loc	string		
yr	int		
	NULL	NULL	
# Partition Information	NULL	NULL	
# col_name            	data_type           	comment             	
	NULL	NULL	
yr	int		

```
- (2)Import 2016 to 2020 data into the partitioned table from table chicago.crime.
- Dynamic Partition
```sql
truncate table crime_parquet_16_20;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table jiayiyang_db.crime_parquet_16_20 partition (yr) 
select id, case_number, `date`, block, iucr, primary_type, description, loc_desc, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, updated_on, latitude, longitude, loc, yr from chicago.crime_parquet WHERE yr>=2016 AND yr<=2020;
SELECT count(*) FROM crime_parquet_16_20 GROUP BY yr;
```
- Manuel Partition
5 times from 2016 to 2020
```sql
insert into table jiayiyang_db.crime_parquet_16_20 partition (yr)
select id,  case_number,  `date`,  block, iucr,  primary_type,  description, 
    loc_desc,  arrest,  domestic,  beat,  district,  ward,  community_area, 
    fbi_code,  x_coordinate,  y_coordinate,  updated_on,  latitude,  longitude,
    loc, 2016 from crime_parquet where yr = 2016;
```


- (3)Write queries to answer following questions:
- Which type of crime is most occurring for each year?  List top 10 crimes for each year.
 ```sql
SELECT b.yr, b.primary_type, num_crime_type, num_crime_rank
FROM(
    SELECT a.yr, a.primary_type, num_crime_type,rank() over( PARTITION BY yr ORDER BY num_crime_type DESC) as num_crime_rank
    FROM(
        SELECT yr, primary_type, count(*) AS num_crime_type
        FROM jiayiyang_db.crime_parquet_16_20
        GROUP BY yr, primary_type) a) b
WHERE num_crime_rank <= 10;
```
```
b.yr,b.primary_type,num_crime_type,num_crime_rank
2016,THEFT,61616,1
2016,BATTERY,50296,2
2016,CRIMINAL DAMAGE,31018,3
2016,DECEPTIVE PRACTICE,19148,4
2016,ASSAULT,18741,5
2016,OTHER OFFENSE,17302,6
2016,BURGLARY,14287,7
2016,NARCOTICS,13317,8
2016,ROBBERY,11960,9
2016,MOTOR VEHICLE THEFT,11287,10
2017,THEFT,64371,1
2017,BATTERY,49230,2
2017,CRIMINAL DAMAGE,29044,3
2017,DECEPTIVE PRACTICE,19444,4
2017,ASSAULT,19305,5
2017,OTHER OFFENSE,17251,6
2017,BURGLARY,12998,7
2017,ROBBERY,11880,8
2017,NARCOTICS,11671,9
2017,MOTOR VEHICLE THEFT,11381,10
2018,THEFT,65261,1
2018,BATTERY,49811,2
2018,CRIMINAL DAMAGE,27822,3
2018,ASSAULT,20402,4
2018,DECEPTIVE PRACTICE,19512,5
2018,OTHER OFFENSE,17249,6
2018,NARCOTICS,13576,7
2018,BURGLARY,11746,8
2018,MOTOR VEHICLE THEFT,9984,9
2018,ROBBERY,9680,10
2019,THEFT,62425,1
2019,BATTERY,49488,2
2019,CRIMINAL DAMAGE,26680,3
2019,ASSAULT,20610,4
2019,DECEPTIVE PRACTICE,18471,5
2019,OTHER OFFENSE,16719,6
2019,NARCOTICS,14800,7
2019,BURGLARY,9629,8
2019,MOTOR VEHICLE THEFT,8979,9
2019,ROBBERY,7989,10
2020,BATTERY,28656,1
2020,THEFT,27755,2
2020,CRIMINAL DAMAGE,16811,3
2020,ASSAULT,12316,4
2020,DECEPTIVE PRACTICE,8726,5
2020,OTHER OFFENSE,8317,6
2020,BURGLARY,6229,7
2020,MOTOR VEHICLE THEFT,6117,8
2020,WEAPONS VIOLATION,5212,9
2020,ROBBERY,5016,10
```
- Which locations are most likely for a crime to happen?  List top 10 locations.
```sql
SELECT loc_desc, count(*) as num_crime_loc
FROM crime_parquet_16_20
GROUP BY loc_desc
ORDER BY num_crime_loc DESC
LIMIT 10;
```
```
loc_desc	num_crime_loc	
STREET	269200	
RESIDENCE	203937	
APARTMENT	160367	
SIDEWALK	95200	
OTHER	45507	
PARKING LOT/GARAGE(NON.RESID.)	32455	
SMALL RETAIL STORE	30814	
RESTAURANT	29501	
ALLEY	23774	
VEHICLE NON-COMMERCIAL	21501	

```
 - Are there certain high crime rate locations for certain crime types? 
```sql
SELECT loc_desc, primary_type,count(*) as num_crime_loc_type
FROM crime_parquet_16_20
GROUP BY loc_desc,primary_type
ORDER BY num_crime_loc_type DESC
LIMIT 10;
```
```
loc_desc,primary_type,num_crime_loc_type
STREET,THEFT,66475
APARTMENT,BATTERY,55980
RESIDENCE,BATTERY,47220
STREET,CRIMINAL DAMAGE,45439
STREET,MOTOR VEHICLE THEFT,34483
STREET,BATTERY,30985
SIDEWALK,BATTERY,30078
RESIDENCE,OTHER OFFENSE,28901
RESIDENCE,DECEPTIVE PRACTICE,27299
RESIDENCE,CRIMINAL DAMAGE,24211

```
## 3.Retail_db dataset:
In retail_db, there are 6 tables.  Get yourself familiar with their schemas.
categories,customers,departments,orders,order_items,products

Write queries to answer following questions:
- (1)List all orders with total order_items = 5.
```sql
SELECT order_id, sum(order_item_quantity)
FROM order_items
JOIN orders
ON order_items.order_item_order_id = orders.order_id
GROUP BY order_id
HAVING sum(order_item_quantity) = 5;
```
5806 records
- (2)List customer_fname，customer_id, order_id, order item_count with total order_items = 5
```sql
SELECT customer_fname, customer_id, orders.order_id, order_item_quantity
FROM orders
JOIN order_items ON orders.order_id = order_items.order_item_order_id
JOIN customers ON orders.order_customer_id = customers.customer_id
WHERE orders.order_id IN 
    (
        SELECT order_id
        FROM order_items
        JOIN orders
        ON order_items.order_item_order_id = orders.order_id
        GROUP BY order_id
        HAVING sum(order_item_quantity) = 5
    );
```
14665 records
- (3)List customer_fname，customer_id, order_id, order item_count with total order_items = 5(join orders, order_items, customers table)
```sql
SELECT customer_fname, customer_id, a.order_id, order_item_quantity
FROM
    (
        SELECT order_id
        FROM order_items
        JOIN orders
        ON order_items.order_item_order_id = orders.order_id
        GROUP BY order_id
        HAVING sum(order_item_quantity) = 5
    ) a
JOIN orders ON a.order_id = orders.order_id
JOIN order_items ON a.order_id = order_items.order_item_order_id
JOIN customers ON customers.customer_id = orders.order_customer_id;
```
14665 records
- (4)List top 10 most popular product categories. (join products, categories,order_items table)
```sql
select category_name, sum(order_item_quantity) as num_sell
from order_items
join products on order_items.order_item_product_id = products.product_id
join categories on product_category_id = category_id
group by category_name
order by num_sell desc
limit 10;
```
```
category_name	num_sell	
Cleats	73734	
Women's Apparel	62956	
Indoor/Outdoor Games	57803	
Cardio Equipment	37587	
Shop By Sport	32726	
Men's Footwear	22246	
Fishing	17325	
Water Sports	15540	
Camping & Hiking	13729	
Electronics	9436	


```
- (5)List top 10 revenue generating products. (join products, orders, order_items table)
```sql
SELECT product_name, sum(order_item_subtotal) as revenue
FROM orders
JOIN order_items ON orders.order_id = order_items.order_item_order_id
JOIN products ON order_items.order_item_product_id = products.product_id
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```
```
product_name	revenue	
Field & Stream Sportsman 16 Gun Fire Safe	6929653.690338135	
Perfect Fitness Perfect Rip Deck	4421143.14352417	
Diamondback Women's Serene Classic Comfort Bi	4118425.570831299	
Nike Men's Free 5.0+ Running Shoe	3667633.196662903	
Nike Men's Dri-FIT Victory Golf Polo	3147800	
Pelican Sunstream 100 Kayak	3099845.085144043	
Nike Men's CJ Elite 2 TD Football Cleat	2891757.6622009277	
O'Brien Men's Neoprene Life Vest	2888993.91355896	
Under Armour Girls' Toddler Spine Surge Runni	1269082.6712722778	
adidas Youth Germany Black/Red Away Match Soc	67830	

```