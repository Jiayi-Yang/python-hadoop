# 1.Employee dataset
In HDFS /data/spark/employee, there are 4 files:dept.txt, dept-with-header.txt, emp.txt, emp-with-header.txt, give employee and dept. information.
Answer following questions by:(1)Spark SQL (2)Spark DataFrame API

```scala
spark.conf.set("spark.sql.shuffle.partitions",2)
val path1 = "/data/spark/employee/dept-with-header.txt"
val dept = spark.read.format("csv").option("header",true).option("inferSchema",true).load(path1)
val path2 = "/data/spark/employee/emp-with-header.txt"
val emp = spark.read.format("csv").option("header",true).option("inferSchema",true).load(path2)
dept.createOrReplaceTempView("dept")
emp.createOrReplaceTempView("emp")
```
```
scala> dept.show
+-------+----------+-------+
|DEPT_NO| DEPT_NAME|    LOC|
+-------+----------+-------+
|     10|ACCOUNTING|NEW YOR|
|     20|  RESEARCH| DALLAS|
|     30|     SALES|CHICAGO|
|     40|OPERATIONS| BOSTON|
+-------+----------+-------+
scala> emp.show
+-----+------+---------+----+-------------------+----+----+------+
|EMPNO|  NAME|      JOB| MGR|           HIREDATE| SAL|COMM|DEPTNO|
+-----+------+---------+----+-------------------+----+----+------+
| 7369| SMITH|    CLERK|7902|1980-12-17 00:00:00| 800|   0|    20|
| 7499| ALLEN| SALESMAN|7698|1981-02-20 00:00:00|1600| 300|    30|
| 7521|  WARD| SALESMAN|7698|1981-02-22 00:00:00|1250| 500|    30|
| 7566| JONES|  MANAGER|7839|1981-04-02 00:00:00|2975|   0|    20|
| 7654|MARTIN| SALESMAN|7698|1981-09-28 00:00:00|1250|1400|    30|
| 7698| BLAKE|  MANAGER|7839|1981-05-01 00:00:00|2850|   0|    30|
| 7782| CLARK|  MANAGER|7839|1981-06-09 00:00:00|2450|   0|    10|
| 7839|  KING|PRESIDENT|null|1981-11-17 00:00:00|5000|   0|    10|
| 7844|TURNER| SALESMAN|7698|1981-09-08 00:00:00|1500|   0|    30|
| 7900| JAMES|    CLERK|7698|1981-12-03 00:00:00| 950|   0|    30|
| 7902|  FORD|  ANALYST|7566|1981-12-03 00:00:00|3000|   0|    20|
| 7934|MILLER|    CLERK|7782|1982-01-23 00:00:00|1300|   0|    10|
+-----+------+---------+----+-------------------+----+----+------+
```
# Spark SQL
## Questions:
### 1.list total salary for each dept.
```
scala> spark.sql("select dept_name,sum(sal) as total_salary from dept join emp on dept.dept_no = emp.deptno group by dept_name").show
+----------+------------+
| dept_name|total_salary|
+----------+------------+
|  RESEARCH|        6775|
|ACCOUNTING|        8750|
|     SALES|        9400|
+----------+------------+
```
### 2.list total number of employee and average salary for each dept.
```
scala> spark.sql("select dept_name,count(*) as total_emp, round(avg(sal)) as avg_salary from dept join emp on dept.dept_no = emp.deptno group by dept_name").show
+----------+---------+----------+
| dept_name|total_emp|avg_salary|
+----------+---------+----------+
|  RESEARCH|        3|    2258.0|
|ACCOUNTING|        3|    2917.0|
|     SALES|        6|    1567.0|
+----------+---------+----------+
```
### 3.list the first hired employee's name for each dept.
```
scala> spark.sql("select deptno,name,hiredate from emp where (deptno,hiredate) in (select deptno,min(hiredate) from dept join emp on dept.dept_no = emp.deptno group by deptno)").show
+------+-----+-------------------+
|deptno| name|           hiredate|
+------+-----+-------------------+
|    20|SMITH|1980-12-17 00:00:00|
|    30|ALLEN|1981-02-20 00:00:00|
|    10|CLARK|1981-06-09 00:00:00|
+------+-----+-------------------+
```
### 4.list total employee salary for each city.
```
scala> spark.sql("select loc as city, sum(sal) as total_salary from dept join emp on dept.dept_no = emp.deptno group by loc").show
+-------+------------+
|   city|total_salary|
+-------+------------+
| DALLAS|        6775|
|CHICAGO|        9400|
|NEW YOR|        8750|
+-------+------------+
```
### 5.list employee's name and salary whose salary is higher than their manager
```
scala> spark.sql("select e.name,e.sal from emp e join emp m on e.mgr=m.empno where e.sal > m.sal").show
+----+----+
|name| sal|
+----+----+
|FORD|3000|
+----+----+
```
### 6.list employee's name and salary whose salary is higher than average salary of whole company
```
scala> spark.sql("select name,sal from emp where sal > (select avg(sal) from emp)").show()
+-----+----+
| name| sal|
+-----+----+
|JONES|2975|
|BLAKE|2850|
|CLARK|2450|
| KING|5000|
| FORD|3000|
+-----+----+
```
### 7.list employee's name and dept name whose name start with "J"
```
scala> spark.sql("select name,dept_name from dept join emp on dept.dept_no = emp.deptno where name like 'J%'").show()
+-----+---------+
| name|dept_name|
+-----+---------+
|JONES| RESEARCH|
|JAMES|    SALES|
+-----+---------+
```
### 8.list 3 employee's name and salary with highest salary
```
scala> spark.sql("select name,sal from emp order by sal desc limit 3").show()
+-----+----+
| name| sal|
+-----+----+
| KING|5000|
| FORD|3000|
|JONES|2975|
+-----+----+
```
### 9.sort employee by total income (salary+commission), list name and total income.
```
scala> spark.sql("select name,sal+comm as total_income from emp order by total_income desc").show()
+------+------------+
|  name|total_income|
+------+------------+
|  KING|        5000|
|  FORD|        3000|
| JONES|        2975|
| BLAKE|        2850|
|MARTIN|        2650|
| CLARK|        2450|
| ALLEN|        1900|
|  WARD|        1750|
|TURNER|        1500|
|MILLER|        1300|
| JAMES|         950|
| SMITH|         800|
+------+------------+
```
# Spark DF (pyspark)
```pyspark
spark.conf.set("spark.sql.shuffle.partitions",2)
path1 = "/data/spark/employee/dept-with-header.txt"
dept = spark.read.format("csv").option("header",True).option("inferSchema",True).load(path1)
path2 = "/data/spark/employee/emp-with-header.txt"
emp = spark.read.format("csv").option("header",True).option("inferSchema",True).load(path2)
```
### 1.list total salary for each dept.
```python
df = emp.join(dept,emp['deptno']==dept['dept_no'])
df.groupby('dept_name').sum('sal').show()
+----------+--------+
| dept_name|sum(sal)|
+----------+--------+
|  RESEARCH|    6775|
|ACCOUNTING|    8750|
|     SALES|    9400|
+----------+--------+
```
### 2.list total number of employee and average salary for each dept.
```python
from pyspark.sql.functions import count,avg,round
df = df.withColumn("sal", df['sal'].cast("int"))
df.groupby('dept_name').agg(round(avg('sal')).alias('avg_salary'),count('*').alias('total_employee')).show()
+----------+----------+--------------+
| dept_name|avg_salary|total_employee|
+----------+----------+--------------+
|  RESEARCH|    2258.0|             3|
|ACCOUNTING|    2917.0|             3|
|     SALES|    1567.0|             6|
+----------+----------+--------------+
```

### 3.list the first hired employee's name for each dept.
```python
from pyspark.sql.functions import min
cond = [df['dept_no']==min_hire_by_dept['deptno'], df['hiredate']==min_hire_by_dept['min_hire']]
df.join(min_hire_by_dept, cond, 'inner').select('name', 'dept_name', 'min_hire').show()
+-----+----------+-------------------+
| name| dept_name|           min_hire|
+-----+----------+-------------------+
|SMITH|  RESEARCH|1980-12-17 00:00:00|
|ALLEN|     SALES|1981-02-20 00:00:00|
|CLARK|ACCOUNTING|1981-06-09 00:00:00|
+-----+----------+-------------------+
```
### 4.list total employee salary for each city.
```python
df.groupby('loc').sum('sal').withColumnRenamed("loc","city").withColumnRenamed("sum(sal)","total_salary").show()
+-------+------------+
|   city|total_salary|
+-------+------------+
| DALLAS|        6775|
|CHICAGO|        9400|
|NEW YOR|        8750|
+-------+------------+
```
### 5.list employee's name and salary whose salary is higher than their manager
```python
m = emp
m = m.select('empno','sal').withColumnRenamed('empno','manager_id').withColumnRenamed('sal','manager_salary')
emp.join(m,emp['mgr']==m['manager_id']).filter(emp['sal']>m['manager_salary']).select(emp['name'], emp['sal']).show()
+----+----+
|name| sal|
+----+----+
|FORD|3000|
+----+----+
```
### 6.list employee's name and salary whose salary is higher than average salary of whole company
```python
avg_sal = emp.agg(avg('sal').alias('avg_salary')).collect()[0][0]
emp.filter(emp['sal']>avg_sal).select('name','sal').show()
+-----+----+
| name| sal|
+-----+----+
|JONES|2975|
|BLAKE|2850|
|CLARK|2450|
| KING|5000|
| FORD|3000|
+-----+----+
```
### 7.list employee's name and dept name whose name start with "J"
```python
df.filter(df['name'].startswith('J')).select('name','dept_name').show()
+-----+---------+
| name|dept_name|
+-----+---------+
|JONES| RESEARCH|
|JAMES|    SALES|
+-----+---------+
```
### 8.list 3 employee's name and salary with highest salary
```python
from pyspark.sql.functions import desc
df.sort(desc('sal')).select('name','sal').show(3)
+-----+----+
| name| sal|
+-----+----+
| KING|5000|
| FORD|3000|
|JONES|2975|
+-----+----+
```
### 9.sort employee by total income (salary+commission), list name and total income.
```python
df = df.withColumn('total_income',df['sal']+df['comm'])
df.sort(desc('total_income')).select('name','total_income').show()
+------+------------+
|  name|total_income|
+------+------------+
|  KING|        5000|
|  FORD|        3000|
| JONES|        2975|
| BLAKE|        2850|
|MARTIN|        2650|
| CLARK|        2450|
| ALLEN|        1900|
|  WARD|        1750|
|TURNER|        1500|
|MILLER|        1300|
| JAMES|         950|
| SMITH|         800|
+------+------------+
```