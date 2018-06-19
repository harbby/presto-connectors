# presto-hbase
presto connector for apache hbase

## Requirements

* Mac OS X or Linux
* Presto 0.203
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)
* hbase client 1.2.0
## Building Presto

Presto-hbase is a standard Maven project. Simply run the following command from the project root directory:

    ./mvn clean package -DskipTests

## Doc

Catalog config
```
connector.name=hbase
hbase.zookeepers=node1:2181,node2:2181,node3:2181
```

创建外部表  如果存在就是映射关系  如果不存在则创建
```
CREATE TABLE a1 (
  rowkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  external = true,
  column_mapping = 'name:info:name,age:info:age,birthday:info:date'
)
```
创建内部表
```
CREATE TABLE a1 (
  rowkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  external = false,
  column_mapping = 'name:info:name,age:info:age,birthday:info:date'
)
```

插入数据
```
INSERT INTO a1 VALUES
('row1', 'Grace Hopper', 109, DATE '1906-12-09' ),
('row2', 'Alan Turing', 103, DATE '1912-06-23' );
```

demo:
```
CREATE TABLE sdk_user (
  rowkey VARCHAR,
  name VARCHAR,
  reg_time VARCHAR
)
WITH (
  external = true,
  column_mapping = 'name:info:name,reg_time:info:reg_time'
)
```
插入json数据
```
create table test1 as SELECT 1 as key,map(ARRAY[1,3], ARRAY[2,4]) as kv;
```
