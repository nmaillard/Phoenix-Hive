#Phoenix-Hive
## Hive StorageHandler for Phoenix 
============
This is a work in progress for a Hive Phoenix connector.
In an ETL workflow id often makes a lot of sense to clean and specialize your data for low latency application needs. In the same way your raw data would move to common data in ORC file format, your application data could end in a Phoenix table for very low latency and concurrent access. This project aims to make this last step as easy and transparent as an INSERT INTO by providing a simple StorageHandler from Hive to Phoenix

### Usage
============

#### [Apache Hive - 13 and Above/ Apache Phoenix 4.2 and above][]

Add phoenix-hive-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/phoenix-hive-<version>.jar;
```
#### Writing Example
To write data from Hive to Phoenix, you must define a table backed by the desired Phoenix table:
here is an example
```SQL
CREATE [EXTERNAL] TABLE phoenix (
    code      STRING,
    description    STRING,
    total_emp INTEGER,
    salary INTEGER,
STORED BY  "org.apache.phoenix.hive.PhoenixStorageHandler"
TBLPROPERTIES(
    'phoenix.hbase.table.name'='sample_test',
    'phoenix.zookeeper.znode.parent'='hbase-unsecure',
    'phoenix.rowkeys'='code',
    'autocreate'='true',
    'autodrop'='true',
    'phoenix.column.mapping'='description:A.description,total_emp:B.total_emp,salary:B.salary'
);
```
In this example we have defined a Hive tabel that stores directly in the corresponding phoenix
```
table.'phoenix.hbase.table.name'='sample_test'
```
Phoenix sits on top of Hbase and leverages Column families if you need it to. By default all columns will be added to a default columne family name 0
Here we decide to precisily write our columns to specific column families for example 2 first columns to CF a and two next columns to CF B
```
'phoenix.column.mapping'='description:A.description,total_emp:B.total_emp,salary:B.salary'
```
Remember the rowkeys do not get written any column family.

Loading
```SQL
INSERT OVERWRITE TABLE phoenix
    select code,description,total_emp,salary
    from sample_07;
```
#### Explanation
Table Type
```
tables can be External or normal.
Normal: Hive manages the table and will try to create and delete it. If Table already exists it will result in an error
External: Hive will try to map to an existing table. In case of Drop will only drop the Hive table definition not Phoenix data.
autocreate: If Exteranl will still try to create it if it does not exist.
autodrop: if External wil still try to drop in Phoenix as well
An External table with autocreate and autodrop is a Normal/Managed table
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
phoenix.hbase.table.name : name of the table in phoenix (if not used will use the Hive tablename)
phoenix.rowkeys = comma separated list of columns to be used in the row key [MANDATORY]
phoenix.column.mapping = comma separated list of mappings from Hive columns to Hive columns
saltbuckets = number of buckets to use for salting the table [ from 1-256]
compression = if table should be compressed [null or gz]
```

### Compile
============
To compile the project 
mvn package -Dhadoop.profile=2
