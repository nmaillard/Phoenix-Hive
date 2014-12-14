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
To write data from Hive to Phoenix, define a table backed by the desired Phoenix table:
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
#### Explanation


### Compile
============
To compile the project 
mvn package -Dhadoop.profile=2
