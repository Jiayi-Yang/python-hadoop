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
create table jiayiyang_db.crime_parquet_16_20_impala (
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
```
- (2)Import 2016 to 2020 data into the partitioned table from table chicago.parquet.
- Dynamic Partition
```sql
truncate table crime_parquet_16_20_impala;
insert into table jiayiyang_db.crime_parquet_16_20_impala partition (yr) 
select id, case_number, `date`, block, iucr, primary_type, description,loc_desc, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, updated_on, latitude, longitude, loc, yr from chicago.crime_parquet WHERE yr>=2016 AND yr<=2020;
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
- Which locations are most likely for a crime to happen?  List top 10 locations.
```sql
SELECT loc_desc, count(*) as num_crime_loc
FROM jiayiyang_db.crime_parquet_16_20
GROUP BY loc_desc
ORDER BY num_crime_loc DESC
LIMIT 10;
```
 - Are there certain high crime rate locations for certain crime types? 
```sql
SELECT loc_desc, primary_type,count(*) as num_crime_loc_type
FROM jiayiyang_db.crime_parquet_16_20
GROUP BY loc_desc,primary_type
ORDER BY num_crime_loc_type DESC
LIMIT 10;
```

## 3.Retail_db dataset:
In retail_db, there are 6 tables.  Get yourself familiar with their schemas.
categories,customers,departments,orders,order_items,products

Write queries to answer following questions:
- (1)List all orders with total order_items = 5.
```sql
REFRESH retail_db.categories;
REFRESH retail_db.customers;
REFRESH retail_db.customers_parquet;
REFRESH retail_db.departments;
REFRESH retail_db.order_items;
REFRESH retail_db.orders;
REFRESH retail_db.products;
```
```sql
SELECT order_id, sum(order_item_quantity)
FROM order_items
JOIN orders
ON order_items.order_item_order_id = orders.order_id
GROUP BY order_id
HAVING sum(order_item_quantity) = 5;
```
Impala only show 1024+ records, impala shell will show 5806 records
- (2)List customer_fname，customer_id, order_id, order item_count with total order_items = 5
```sql
compute stats retail_db.categories;
compute stats retail_db.customers;
compute stats retail_db.customers_parquet;
compute stats retail_db.departments;
compute stats retail_db.order_items;
compute stats retail_db.orders;
compute stats retail_db.products;

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
Impala only show 1024+ records, impala shell will show 5806 records
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
