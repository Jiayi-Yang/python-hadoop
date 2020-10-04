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
# Spark DF
### 1.list total salary for each dept.
### 2.list total number of employee and average salary for each dept.
### 3.list the first hired employee's name for each dept.
### 4.list total employee salary for each city.
### 5.list employee's name and salary whose salary is higher than their manager
