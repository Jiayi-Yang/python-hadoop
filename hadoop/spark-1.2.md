## 1.Banklist dataset
Write queries on banklist table:
- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
```python
spark.conf.set("spark.sql.shuffle.partitions",2)
bank = spark.read.table("jiayiyang_db.banklists2")
bank.groupby('st').agg(count("*").alias("num_banks")).sort(desc("num_banks")).show(5)
+---+---------+
| st|num_banks|
+---+---------+
| GA|       93|
| FL|       75|
| IL|       69|
| CA|       41|
| MN|       23|
+---+---------+
```
- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```python
bank.withColumn('close_year',year(bank['closing_date'])).groupby('close_year').agg(count("*").alias("num_close")).sort("close_year").show()
+----------+---------+
|close_year|num_close|
+----------+---------+
|      2000|        2|
|      2001|        4|
|      2002|       11|
|      2003|        3|
|      2004|        4|
|      2007|        3|
|      2008|       25|
|      2009|      140|
|      2010|      157|
|      2011|       92|
|      2012|       51|
|      2013|       24|
|      2014|       18|
|      2015|        8|
|      2016|        5|
|      2017|        8|
|      2019|        4|
|      2020|        2|
+----------+---------+
```
## 2.Chicago crime dataset:
- (1)In your own database, create a partitioned table(partitioned by year)to store data, store in parquet format.Name the table “crime_parquet_16_20”;
- (2)Import 2016 to 2020 data into the partitioned table from table chicago.parquet.
- TODO
- (3)Write queries to answer following questions:
- Which type of crime is most occurring for each year?  List top 10 crimes for each year.
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,year,desc,count
crime = spark.read.table("jiayiyang_db.crime_parquet_16_20")
windowSpec  = Window.partitionBy("yr").orderBy(desc('num_crime_type'))
crime_rank = crime.groupby('yr','primary_type').agg(count("*").alias('num_crime_type')).withColumn("rk",rank().over(windowSpec))
crime_rank.filter(crime_rank["rk"]<=10).show()
+----+-------------------+--------------+---+
|  yr|       primary_type|num_crime_type| rk|
+----+-------------------+--------------+---+
|2018|              THEFT|         65261|  1|
|2018|            BATTERY|         49811|  2|
|2018|    CRIMINAL DAMAGE|         27822|  3|
|2018|            ASSAULT|         20402|  4|
|2018| DECEPTIVE PRACTICE|         19512|  5|
|2018|      OTHER OFFENSE|         17249|  6|
|2018|          NARCOTICS|         13576|  7|
|2018|           BURGLARY|         11746|  8|
|2018|MOTOR VEHICLE THEFT|          9984|  9|
|2018|            ROBBERY|          9680| 10|
|2019|              THEFT|         62425|  1|
|2019|            BATTERY|         49488|  2|
|2019|    CRIMINAL DAMAGE|         26680|  3|
|2019|            ASSAULT|         20610|  4|
|2019| DECEPTIVE PRACTICE|         18471|  5|
|2019|      OTHER OFFENSE|         16719|  6|
|2019|          NARCOTICS|         14800|  7|
|2019|           BURGLARY|          9629|  8|
|2019|MOTOR VEHICLE THEFT|          8979|  9|
|2019|            ROBBERY|          7989| 10|
+----+-------------------+--------------+---+
only showing top 20 rows
```
- Which locations are most likely for a crime to happen?  List top 10 locations.
```python
crime.groupby("loc_desc").agg(count("*").alias("num_crime_loc")).sort(desc("num_crime_loc")).show(10)
+--------------------+-------------+
|            loc_desc|num_crime_loc|
+--------------------+-------------+
|              STREET|       269200|
|           RESIDENCE|       203937|
|           APARTMENT|       160367|
|            SIDEWALK|        95200|
|               OTHER|        45507|
|PARKING LOT/GARAG...|        32455|
|  SMALL RETAIL STORE|        30814|
|          RESTAURANT|        29501|
|               ALLEY|        23774|
|VEHICLE NON-COMME...|        21501|
+--------------------+-------------+
only showing top 10 rows
```
- Are there certain high crime rate locations for certain crime types? 
```python
crime.groupby("loc_desc","primary_type").agg(count("*").alias("num_crime")).sort(desc("num_crime")).show(10)
+---------+-------------------+---------+
| loc_desc|       primary_type|num_crime|
+---------+-------------------+---------+
|   STREET|              THEFT|    66475|
|APARTMENT|            BATTERY|    55980|
|RESIDENCE|            BATTERY|    47220|
|   STREET|    CRIMINAL DAMAGE|    45439|
|   STREET|MOTOR VEHICLE THEFT|    34483|
|   STREET|            BATTERY|    30985|
| SIDEWALK|            BATTERY|    30078|
|RESIDENCE|      OTHER OFFENSE|    28901|
|RESIDENCE| DECEPTIVE PRACTICE|    27299|
|RESIDENCE|    CRIMINAL DAMAGE|    24211|
+---------+-------------------+---------+
``` 
## 3.Retail_db dataset:
In retail_db, there are 6 tables.  Get yourself familiar with their schemas.
categories,customers,departments,orders,order_items,products

Write queries to answer following questions:
- (1)List all orders with total order_items = 5.
```python
order_items = spark.read.table("retail_db.order_items")
order_temp = order_items.groupby("order_item_order_id").agg(count("*").alias("item_counts"))
order_temp.filter(order_temp["item_counts"]==5).sort("order_item_order_id").select("order_item_order_id","item_counts").show()
+-------------------+-----------+
|order_item_order_id|item_counts|
+-------------------+-----------+
|                  5|          5|
|                 10|          5|
|                 11|          5|
|                 12|          5|
|                 15|          5|
|                 17|          5|
|                 24|          5|
|                 28|          5|
|                 29|          5|
|                 58|          5|
|                 64|          5|
|                 73|          5|
|                 77|          5|
|                 84|          5|
|                107|          5|
|                110|          5|
|                112|          5|
|                116|          5|
|                122|          5|
|                140|          5|
+-------------------+-----------+
only showing top 20 rows
```
- (2)list customer_id, order_id, order item_count with total order_items = 5
```python
orders = spark.read.table("retail_db.orders")
order_join = order_items.join(orders,order_items['order_item_order_id']==orders['order_id'])
order_temp = order_join.groupby("order_customer_id","order_item_order_id").agg(count("*").alias("item_counts"))
order_temp.filter(order_temp["item_counts"]==5).sort("order_item_order_id").select("order_customer_id","order_item_order_id","item_counts").show()
+-----------------+-------------------+-----------+
|order_customer_id|order_item_order_id|item_counts|
+-----------------+-------------------+-----------+
|            11318|                  5|          5|
|             5648|                 10|          5|
|              918|                 11|          5|
|             1837|                 12|          5|
|             2568|                 15|          5|
|             2667|                 17|          5|
|            11441|                 24|          5|
|              656|                 28|          5|
|              196|                 29|          5|
|             9213|                 58|          5|
|             5579|                 64|          5|
|             8504|                 73|          5|
|             7915|                 77|          5|
|             6789|                 84|          5|
|             1845|                107|          5|
|             2746|                110|          5|
|             5375|                112|          5|
|             8763|                116|          5|
|             2071|                122|          5|
|             4257|                140|          5|
+-----------------+-------------------+-----------+
only showing top 20 rows
```
- (3)List customer_fname，customer_id, order_id, order item_count with total order_items = 5(join orders, order_items, customers table)
```python
customers = spark.read.table("retail_db.customers")
order_join = order_items.join(orders,order_items['order_item_order_id']==orders['order_id']).join(customers,customers['customer_id']==orders['order_customer_id'])
order_temp = order_join.groupby("order_customer_id","customer_fname","order_item_order_id").agg(count("*").alias("item_counts"))
order_temp.filter(order_temp["item_counts"]==5).sort("order_item_order_id").select("order_customer_id","customer_fname","order_item_order_id","item_counts").show()
+-----------------+--------------+-------------------+-----------+
|order_customer_id|customer_fname|order_item_order_id|item_counts|
+-----------------+--------------+-------------------+-----------+
|            11318|          Mary|                  5|          5|
|             5648|        Joshua|                 10|          5|
|              918|        Nathan|                 11|          5|
|             1837|          Mary|                 12|          5|
|             2568|         Maria|                 15|          5|
|             2667|         Tammy|                 17|          5|
|            11441|          Mary|                 24|          5|
|              656|         Julie|                 28|          5|
|              196|        Thomas|                 29|          5|
|             9213|      Jennifer|                 58|          5|
|             5579|          Mary|                 64|          5|
|             8504|          Mary|                 73|          5|
|             7915|        Justin|                 77|          5|
|             6789|       Jessica|                 84|          5|
|             1845|          Mary|                107|          5|
|             2746|     Stephanie|                110|          5|
|             5375|        Jeremy|                112|          5|
|             8763|        Pamela|                116|          5|
|             2071|        Walter|                122|          5|
|             4257|       William|                140|          5|
+-----------------+--------------+-------------------+-----------+
only showing top 20 rows
```
- (4)List top 10 most popular product categories. (join products, categories,order_items table)
```
products = spark.read.table("retail_db.products")
categories = spark.read.table("retail_db.categories")
order_items.join(products,order_items['order_item_product_id']==products['product_id']).join(categories,categories['category_id']==products['product_category_id']).groupby("category_name").agg(count("*").alias("item_counts")).sort(desc("item_counts")).select("category_name","item_counts").show(10)
+--------------------+-----------+
|       category_name|item_counts|
+--------------------+-----------+
|              Cleats|      24551|
|      Men's Footwear|      22246|
|     Women's Apparel|      21035|
|Indoor/Outdoor Games|      19298|
|             Fishing|      17325|
|        Water Sports|      15540|
|    Camping & Hiking|      13729|
|    Cardio Equipment|      12487|
|       Shop By Sport|      10984|
|         Electronics|       3156|
+--------------------+-----------+
only showing top 10 rows
```
- (5)List top 10 revenue generating products. (join products, orders, order_items table)
```python

```