#Phoenix-Hive
## PhoenixStorageHandler for Hive 
============
This is a work in progress for a Hive PhoenixStorageHandler.
In an ETL workflow id often makes a lot of sense to clean and specialize your data for low latency application needs. In the same way your raw data would move to common data in ORC file format, your application data could end in a Phoenix table for very low latency and concurrent access. This project aims to make this last step as easy and transparent as an INSERT INTO by providing a simple StorageHandler from Hive to Phoenix

### Limitations
============
* Only works with MapReduce as execution engine (P1)
* Only supports Hive Primitive types, essentially missing Array Type for Phoenix (P0)
* Very limited predicate pushdown will get better in next version (P0)
* No work yet in a secured environment

### Usage
============

#### [Apache Hive - 13 and Above/ Apache Phoenix 4.2 and above]

Add phoenix-hive-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/phoenix-hive-<version>.jar;
```
#### Writing Example
To write data from Hive to Phoenix, you must define a table backed by the desired Phoenix table:
here is an example
##### Creating table
```SQL
CREATE EXTERNAL TABLE phoenix_table( 
	ID int,
	code STRING,
	description STRING,
	total INT,
	SALARY INT)
STORED BY  "org.apache.phoenix.hive.PhoenixStorageHandler"
TBLPROPERTIES(
    'phoenix.hbase.table.name'='phoenix_table',
    'phoenix.zookeeper.znode.parent'='hbase-unsecure',
    'phoenix.rowkeys'='id,code',
    'autocreate'='true',
    'autodrop'='true',
    'phoenix.column.mapping'='description:A.description,total:B.total,salary:B.salary'
);
```
In this example we have defined a Hive table that stores directly in the corresponding phoenix table
```
'phoenix.hbase.table.name'='phoenix_table'
```
Phoenix sits on top of Hbase and leverages Column families if you need it to. By default all columns will be added to a default columne family name 0
Here we decide to precisily write our columns to specific column families for example 2 first columns to CF a and two next columns to CF B
```
'phoenix.column.mapping'='description:A.description,total_emp:B.total_emp,salary:B.salary'
```
Remember the rowkeys do not get written any column family.

##### Loading
```SQL
INSERT INTO TABLE phoenix_table
    select 10,code,description,total_emp,salary
    FROM sample_07;
    
INSERT INTO TABLE phoenix_table
    select split(code, '-')[1],code,description,total_emp,salary
    FROM sample_07;
```
#### Explanation
Table Type
```
tables can be External or Managed.
Managed: Hive manages the table and will try to create and delete it. If Table already exists it will result in an error
External: Hive will try to map to an existing table. In case of Drop will only drop the Hive table definition not Phoenix data.
- autocreate: If Exteranl will still try to create it if it does not exist.
- autodrop: if External wil still try to drop in Phoenix as well
An External table with autocreate and autodrop set is a Managed table
```
TBLPROPERTIES
Cluster Properties
```
phoenix.zookeeper.quorum: comma separated list of machines in zookeeper quorum [default: localhost]
phoenix.zookeeper.znode.parent: znode for zookeeper [default: hbase]
phoenix.zookeeper.client.port: zookeeper port [default: 2181]
```
Table Properties
```
phoenix.hbase.table.name : name of the table in phoenix [default: name of hive table]
phoenix.rowkeys = comma separated list of columns to be used in the row key [MANDATORY]
phoenix.column.mapping = comma separated list of mappings from Hive columns to Hive columns [default: hive cols names]
saltbuckets = number of buckets to use for salting the table [ from 1-256]
```
#### Reading Example
To read data from Phoenix through Hive interface works like any other Hive select query:
##### Reporting query
```SQL
Select * from phoenix_table
Select code,description from phoenix_table
Select count(*) from phoenix_table
Select id,count(*) from phoenix_table group by id
```
##### Joining Phoenix and Hive data
```SQL
select * from phoenix_table pt,sample_07 s where pt.code=s.code
select pt.id,s.code,pt.salary,s.salary from phoenix_table pt,sample_07 s where pt.code=s.code and s.salary>33000;
```
##### Joining 2 phoenix tables
```SQL
select pt.id,s.code,pt.salary,s.salary from phoenix_table pt,phoenix_sample ps s where pt.code=ps.code and ps.salary>33000;
```
### Compile
============
To compile the project 
mvn package -Dhadoop.profile=2 
