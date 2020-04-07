# 数栈 SPI

## 一、介绍
模拟 Driver 通过不同的 classloader 加载，避免类名冲突导致加载不上的问题。

## 二、使用配置

> 将打包完的 pluginLibs 下需要的插件放到对应 jar 包的 root 目录（ 目录可以通过AbsClientCache.setUserDir 重新定义）
> 并以 pluginLibs 为外目录，例如：/home/admin/app/dt-center-ide/pluginLibs/clickhouse/dtClickhouse.jar

---

## 三、已支持数据源（具体版本后续补充）

### 3.1 关系型数据库
* Mysql5、8
* ADS
* DRDS
* Oracle
* SQLServer2012、2017
* PostgreSQL
* Libra
* MaxComputer
* DB2
* HDFS
* Hive
* Hive1.X
* ClickHouse
* GBase
* HBase
* Impala
* Kylin
* Kudu

### 3.2 非关系型数据库
* Redis
* MongoDB
* Kafka 0.9、0.10、0.11、1.x版本
* ES5、ES6

### 3.3 其他
* FTP

---
## 四、已支持方法

### 4.1 支持的函数

| 方法           | 入参                    | 出参                        | 备注  |
|---------------|------------------------|-----------------------------|------|
| getConn      | SourceDTO             | Connection                | 获取 连接    |
| testConn     | SourceDTO             | Boolean                   | 校验 连接    |
| executeQuery | SourceDTO,SqlQueryDTO | List<Map<String, Object>> | 执行查询     |
| executeSqlWithoutResultSet | SourceDTO,SqlQueryDTO | Boolean | 执行查询，无需结果集     |
| getTableList | SourceDTO,SqlQueryDTO | List<String>              | 获取数据库表名称 |
| getColumnClassInfo | SourceDTO,SqlQueryDTO | List<String>              | 获取字段 Java 类的标准名称,字段名若不填则默认全部 |
| getColumnMetaData | SourceDTO,SqlQueryDTO | List<ColumnMetaDTO>              | 获取字段属性 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getTableMetaComment | SourceDTO,SqlQueryDTO | List<ColumnMetaDTO>              | 获取表备注信息 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getAllBrokersAddress | SourceDTO | String | 获取所有 Brokers 的地址 |
| getTopicList | SourceDTO | List<String> | 获取所有 Topic 信息 |
| createTopic | SourceDTO,KafkaTopicDTO | Boolean | 创建 Topic |
| getAllPartitions | SourceDTO,String | List<ColumnMetaDTO>              | 获取特定 Topic 分区信息 |
| getColumnMetaData | SourceDTO,SqlQueryDTO | List<MetadataResponse.PartitionMetadata>| 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤 |
| getOffset | SourceDTO,String | KafkaOffsetDTO | 获取特定 Topic 位移量|

**备注**

[SourceDTO](http://gitlab.prod.dtstack.cn/dt-insight-web/dt-center-common-loader/blob/master/core/src/main/java/com/dtstack/dtcenter/loader/dto/SourceDTO.java)

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

---

## 六、开发步骤说明
### 6.1 自身优化
#### 6.1.1 增加支持方法
1. com.dtstack.dtcenter.loader.client.IClient 中增加对应的方法
2. com.dtstack.dtcenter.loader.client.sql.DataSourceClientProxy 中增加代理实现
3. 对应的抽象类和具体实现类补充对应的方法实现

#### 6.1.2 增加支持的数据源
1. 在对应的关系型数据库模块或者非关系型模块增加子模块并按照其他模块修改对应的pom
2. 继承对应抽象类并重写对应方法
3. 在Resources/META-INF/services 下增加文件com.dtstack.dtcenter.loader.client.IClient，并在里面补充实现类的引用地址：例如：com.dtstack.dtcenter.common.loader.rdbms.db2.Db2Client

### 6.2 二次开发使用

#### 6.2.1 配置 maven
```$xml
<dependency>
    <groupId>com.dtstack.dtcenter</groupId>
    <artifactId>common.loader.core</artifactId>
    <version>1.0-SNAPSHOT</version>
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
    IClient client = clientCache.getClient(DataSourceClientType.MySql5.getPluginName());
        SourceDTO source = SourceDTO.builder()
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
        IClient client = clientCache.getClient(DataSourceClientType.MySql5.getPluginName());
        SourceDTO source = SourceDTO.builder()
            .url("jdbc:mysql://172.16.8.109:3306/ide")
            .username("dtstack")
            .password("abc123")
            .build();
        Boolean isConnect = client.testConn(source);
    }
```
