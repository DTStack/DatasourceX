# 数据源插件 common-loader

## 一、介绍
模拟 Driver 通过不同的 classloader 加载，避免类名冲突导致加载不上的问题，提供大量关系型数据库、非关系型数据库以及像 HDFS、S3、ftp数据源的一些通用方法，简化开发成本：

- 大部分关系型数据库支持开启连接池以提高并发性能[连接池使用](docs/connectionPool.md)

- 部分数据源支持kerberos认证[kerberos认证](docs/kerberos.md)

---

## 二、已支持的数据源

### 2.1 关系型数据库

| Database Type               | Client doc                                   |
|:---------------------------:|:---------------------------------------------:|
| MySQL5                     | [doc](docs/rdbms/mysql5.md)               |
| MySQL8                     | [doc](docs/rdbms/mysql8.md)               |
| Polardb_For_MySQL(同mysql5) | [doc](docs/rdbms/mysql5.md)               |
| Oracle                     | [doc](docs/rdbms/oracle.md)               |
| SQLServer                  | [doc](docs/rdbms/sqlserver.md)            |
| PostgreSQL                 | [doc](docs/rdbms/postgresql.md)           |
| DB2                        | [doc](docs/rdbms/rdbm.md)                  |
| DMDB                       | [doc](docs/rdbms/rdbm.md)                 |
| KINGBASE8                  | [doc](docs/rdbms/rdbm.md)            |
| HIVE1.X                    | [doc](docs/rdbms/hive.md)                |
| HIVE2.X                    | [doc](docs/rdbms/hive.md)                |
| SPARK                      | [doc](docs/rdbms/hive.md)                |
| IMPALA                     | [doc](docs/rdbms/rdbm.md)               |
| Clickhouse                 | [doc](docs/rdbms/rdbm.md)           |
| TiDB(同mysql5)             | [doc](docs/rdbms/mysql5.md)               |
| CarbonData(同hive2.X)      | [doc](docs/rdbms/hive.md)                |
| Kudu                       | [doc](docs/rdbms/kudu.md)                 |
| ADS(同mysql5)              | [doc](docs/rdbms/mysql5.md)               |
| Kylin                      | [doc](docs/rdbms/rdbm.md)                |
| Libra                      | [doc](docs/rdbms/libra.md)                |
| GREENPLUM6                 | [doc](docs/rdbms/greenplum6.md)           |
| GBase_8a                   | [doc](docs/rdbms/rdbm.md)                |
| Phoenix4                   | [doc](docs/rdbms/rdbm.md)             |
| Phoenix5                   | [doc](docs/rdbms/rdbm.md)             |
| Vertica                    | [doc](docs/rdbms/rdbm.md)              |

### 2.2 非关系型数据库

| Database Type               | Client doc                                   |
|:---------------------------:|:---------------------------------------------:|
| REDIS                       | [doc](docs/nosql/redis.md)              |
| HBASE(hbase1.x、2.x版本)     | [doc](docs/nosql/hbase.md)               |
| ES(ES6、ES7版本)             | [doc](docs/nosql/es.md)                  |
| MONGODB                     | [doc](docs/nosql/mongodb.md)             |
| HBASE                       | [doc](docs/nosql/hbase.md)               |

### 2.3 文件系统

| Database Type               | Client doc                                   |
|:---------------------------:|:---------------------------------------------:|
| HDFS                        | [doc](docs/fs/hdfs.md)                   |
| S3(此为中国移动版本)           | [doc](docs/fs/s3.md)                     |
| AWS_S3                      | [doc](docs/fs/aws_s3.md)                 |
| FTP                         | [doc](docs/fs/ftp.md)                    |

### 2.4 消息队列

| Database Type               | Client doc                                   |
|:------------------------------:|:------------------------------------------:|
| KAFKA(0.9、0.10、0.11、1.x版本)  | [doc](docs/mq/kafka.md)               |
| EMQ                            | [doc](docs/mq/emq.md)                 |

### 2.5 其他

| Database Type               | Client doc                                   |
|:------------------------------:|:------------------------------------------:|
| websocket                      | [doc](docs/other/websocket.md)        |
| socket                         | [doc](docs/other/socket.md)           |

---

## 三、立刻使用

请点击[立刻使用](docs/nowstart.md)

---

## 四、kerberos认证

请点击[kerberos认证](docs/kerberos.md)

---

## 五、开发步骤说明
1. 在core模块下com.dtstack.dtcenter.loader.source.DataSourceType中增加新的数据源信息
2. 在core模块下com.dtstack.dtcenter.loader.dto.source包中创建新数据源对应的sourceDTO，继承对应的抽象类或实现ISourceDTO接口
3. 在项目下增加子模块并按照其他模块修改项目和子模块对应的pom文件信息
4. 在新创建的模块中创建client类继承对应抽象类或实现对应接口并重写需要实现的方法
5. 在Resources/META-INF/services 下增加文件com.dtstack.dtcenter.loader.client.IClient，并在里面补充实现类的引用地址：例如：com.dtstack.dtcenter.common.loader.db2.Db2Client