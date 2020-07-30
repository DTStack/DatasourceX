# 数栈 SPI

## 一、介绍
模拟 Driver 通过不同的 classloader 加载，避免类名冲突导致加载不上的问题。

## 二、使用配置

> 将打包完的 pluginLibs 下需要的插件放到对应 jar 包的 root 目录（ 目录可以通过AbsClientCache.setUserDir 重新定义）
> 并以 pluginLibs 为外目录，例如：/home/admin/app/dt-center-ide/pluginLibs/clickhouse/dtClickhouse.jar

---

## 三、已支持数据源（具体版本后续补充）

### 3.1 关系型数据库
* ClickHouse
* DB2
* DM
* DRDS
* FTP
* GBase
* Greenplum 6.X
* HBase
* HDFS
* Hive
* Hive 1.X
* Impala
* Kudu
* Kylin
* Libra
* MaxComputer
* Mysql 5、8
* Oracle
* Phoneix
* PostgreSQL
* SQLServer2012、2017

### 3.2 非关系型数据库
* Redis
* MongoDB
* ES5、ES6

### 3.3 消息队列
* Kafka 0.9、0.10、0.11、1.x版本
* EMQ

### 3.4 其他
* FTP

---
## 四、已支持方法

### 4.1 支持的函数

#### IClient
| 方法           | 入参                    | 出参                        | 备注  |
|---------------|------------------------|-----------------------------|------|
| getConn      | 对应数据源的DTO             | Connection                | 获取 连接    |
| testConn     | 对应数据源的DTO             | Boolean                   | 校验 连接    |
| executeQuery | 对应数据源的DTO,SqlQueryDTO | List<Map<String, Object>> | 执行查询     |
| executeSqlWithoutResultSet | 对应数据源的DTO,SqlQueryDTO | Boolean | 执行查询，无需结果集     |
| getTableList | 对应数据源的DTO,SqlQueryDTO | List<String>              | 获取数据库表名称 |
| getColumnClassInfo | 对应数据源的DTO,SqlQueryDTO | List<String>              | 获取字段 Java 类的标准名称,字段名若不填则默认全部 |
| getColumnMetaData | 对应数据源的DTO,SqlQueryDTO | List<ColumnMetaDTO>              | 获取字段属性 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getFlinkColumnMetaData | 对应数据源的DTO,SqlQueryDTO | List<ColumnMetaDTO>              | 获取flinkSql任务字段属性 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getTableMetaComment | 对应数据源的DTO,SqlQueryDTO | List<ColumnMetaDTO>              | 获取表备注信息 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getPreview | 对应数据源的DTO,SqlQueryDTO | List<List<Object>>              | 获取预览数据,支持大多数关系型数据库，支持es、mongdb |

#### IKafka
| 方法           | 入参                    | 出参                        | 备注  |
|---------------|------------------------|-----------------------------|------|
| testConn     | 对应数据源的DTO             | Boolean                   | 校验 连接    |
| getAllBrokersAddress | 对应数据源的DTO | String | 获取所有 Brokers 的地址 |
| getTopicList | 对应数据源的DTO | List<String> | 获取所有 Topic 信息 |
| createTopic | 对应数据源的DTO,KafkaTopicDTO | Boolean | 创建 Topic |
| getAllPartitions | 对应数据源的DTO,String | List<ColumnMetaDTO>              | 获取特定 Topic 分区信息 |
| getColumnMetaData | 对应数据源的DTO,SqlQueryDTO | List<MetadataResponse.PartitionMetadata>| 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getOffset | 对应数据源的DTO,String | KafkaOffsetDTO | 获取特定 Topic 位移量|
| getPreview | 对应数据源的DTO,SqlQueryDTO | List<List<Object>>              | 获取预览数据 |

**备注**

[sourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source)

* [ISourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/ISourceDTO.java)
* [RdbmsSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/RdbmsSourceDTO.java)
* [ClickHouseSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/ClickHouseSourceDTO.java)
* [Db2SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Db2SourceDTO.java)
* [DmSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/DmSourceDTO.java)
* [GBaseSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/GBaseSourceDTO.java)
* [Greenplum6SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Greenplum6SourceDTO.java)
* [HbaseSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/HbaseSourceDTO.java)
* [HdfsSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/HdfsSourceDTO.java)
* [Hive1SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Hive1SourceDTO.java)
* [HiveSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/HiveSourceDTO.java)
* [ImpalaSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/ImpalaSourceDTO.java)
* [KuduSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/KuduSourceDTO.java)
* [KylinSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/KylinSourceDTO.java)
* [LibraSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/LibraSourceDTO.java)
* [Mysql5SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Mysql5SourceDTO.java)
* [Mysql8SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Mysql8SourceDTO.java)
* [OdpsSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/OdpsSourceDTO.java)
* [OracleSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/OracleSourceDTO.java)
* [PhoenixSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/PhoenixSourceDTO.java)
* [PostgresqlSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/PostgresqlSourceDTO.java)
* [Sqlserver2017SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Sqlserver2017SourceDTO.java)
* [SqlserverSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/SqlserverSourceDTO.java)
* [RedisSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/RedisSourceDTO.java)
* [MongoSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/MongoSourceDTO.java)
* [ESSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/ESSourceDTO.java)
* [KafkaSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/KafkaSourceDTO.java)
* [EMQSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/EMQSourceDTO.java)
* [FtpSourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/FtpSourceDTO.java)

[SqlQueryDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/SqlQueryDTO.java)

[ColumnMetaDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

[KafkaTopicDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/KafkaTopicDTO.java)

[KafkaOffsetDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/KafkaOffsetDTO.java)

### 4.2 复用 Connection 使用说明
[CacheConnectionHelper](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/cache/connection/CacheConnectionHelper.java)
1. 先使用 start 开启缓存连接池配置，如果有要求多次请求复用，请传入唯一 sessionKey
2. 使用完请使用 stop 关闭缓存，如果使用了 start 则必须使用 stop 关闭线程池，否则会存储到缓存池中，轮询超时销毁
3. 如果 VertX 等服务有线程池的情况，需要再请求技术之后，不管有没有 stop 都需要一次 removeCacheConnection()

### 4.3 插件文件校验
[AbsClientCache](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/client/AbsClientCache.java)
如果需要开启插件校验，请使用 startCheckFile 来校验文件
默认十分钟一次校验，首次开启不校验，用到的缓存连接才会校验

### 4.4 方法说明
1. getPreview方法和getColumnMetaData方法中tableName处理方法：
1). sqlQueryDTO中的tableName为必填项。
2). 对于sqlServer数据源，如果传过来的是不带schema的表名，则使用[]处理；
3). 如果传过来tableName被[]包括，则不处理；
4). 如果传过来的schema.tableName格式，则进行最大长度2切割，此时tableName中即使有"."或者被[]包裹，也不影响系统；
ps: 如果直接传过来的表名中有"."，请使用[]进行包裹处理

---
## 五、后续开发计划
1. IClient DTO 优化，为 Kerberos 和 复用 Connection 准备 【已完成 200226】
2. Kerberos 支持，需要支持注解、支持 Start & Close【已完成,分析发现直接在 SourceDTO 中设置效果更佳 200226】
3. 支持 HDFS、Kudu、ES 这三个数据源 【已完成】
4. SQLServer 分版本支持，支持 SQLServer 2012、2008、2017&Later 【已完成】
5. Mysql 分版本支持，支持 Mysql 5 及 Mysql 8、TiDB 兼容，使用 Mysql 支持 【已完成】
6. Phoenix 常用版本支持 【已完成】
7. 复用 Connection 支持，需要支持注解、支持 Start & Close 【已完成】
8. 其他非关系型数据库支持、关系型数据库支持、不同版本支持，根据项目需要支持
9. 优化异常信息，将异常抛出放到 core 中
10. Hadoop 版本放入 loader 包不同插件自身去控制
11. 实现 common 中的 IDownLoad
12. hive、odps数据库支持数据分区预览功能【已完成】
---

## 六、开发步骤说明
### 6.1 自身优化
#### 6.1.1 增加支持方法
1. com.dtstack.dtcenter.loader.client.IClient 中增加对应的方法
2. com.dtstack.dtcenter.loader.client.sql.DataSourceClientProxy 中增加代理实现
3. 对应的抽象类和具体实现类补充对应的方法实现
4. 支持查询数据源的所有 DB
5. 支持单独查询表的分区字段
6. 支持获取建表语句
7. kafka逻辑从IClient剥离

#### 6.1.2 增加支持的数据源
1. 在对应的关系型数据库模块或者非关系型模块增加子模块并按照其他模块修改对应的pom
2. 继承对应抽象类并重写对应方法
3. 在Resources/META-INF/services 下增加文件com.dtstack.dtcenter.loader.client.IClient，并在里面补充实现类的引用地址：例如：com.dtstack.dtcenter.common.loader.db2.Db2Client

### 6.2 二次开发使用

#### 6.2.1 配置 maven
```$xml
<dependency>
    <groupId>com.dtstack.dtcenter</groupId>
    <artifactId>common-loader</artifactId>
    <version>1.2.0-SNAPSHOT</version>
</dependency>
```

#### 6.2.2 具体使用
分为两种方案：

1. 类似于 DriverManger.getConnection 类似，直接使用 Connection 去做二次开发使用
```$Java
    // 历史方法
    String url = "jdbc:mysql://172.16.8.109:3306/ide";
    Properties prop = new Properties();
    prop.put("user", "dtstack");
    prop.put("password", "abc123");

    Class.forName(dataBaseType.getDriverClassName());
    Connection clientCon = DriverManager.getConnection(url, prop);

    // 改为
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlConnection() throws Exception {
    IClient client = clientCache.getClient(DataSourceType.MySql5.getPluginName());
        Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.8.109:3306/ide")
            .username("dtstack")
            .password("abc123")
            .build();
        Connection clientCon = client.getCon(source);
    }
```

2. 直接使用工具封装的方法，具体见第四点
```$java
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlConnection() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySql5.getPluginName());
        Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://172.16.8.109:3306/ide")
            .username("dtstack")
            .password("abc123")
            .build();
        Boolean isConnect = client.testConn(source);
    }
```
```$java
    //kafka客户端插件需要如下使用
    private static final AbsClientCache kafkaClientCache = ClientType.KAFKA_CLIENT.getClientCache();

    @Test
    public void getTopicList() throws Exception {
        IKafka client = kafkaClientCache.getKafka(DataSourceType.KAFKA_09.getPluginName());
        KafkaSourceDTO source = KafkaSourceDTO.builder()
                        .url("172.16.8.107:2181/kafka")
                        .build();
        List<String> topicList = client.getTopicList(source);
        
    }
```
