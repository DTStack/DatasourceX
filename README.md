# 数栈 SPI

## 一、介绍
模拟 Driver 通过不同的 classloader 加载，避免类名冲突导致加载不上的问题。

## 二、使用配置

> 将打包完的 pluginLibs 下需要的插件放到对应 jar 包的 root 目录，并已pluginLibs 为外目录，例如：/home/admin/app/dt-center-ide/pluginLibs/clickhouse/dtClickhouse.jar

---

## 三、已支持数据源（具体版本后续补充）

### 3.1 关系型数据库
* ClickHouse
* DB2
* DRDS
* GBase
* Hive
* Hive1.X
* Impala
* Kylin
* MaxComputer
* Mysql
* Oracle
* PostgreSQL
* SQLServer

### 3.2 非关系型数据库
* Redis
* MongoDB

---
## 四、已支持方法

| 方法           | 入参                    | 出参                        | 备注       |
|--------------|-----------------------|---------------------------|----------|
| getConn      | SourceDTO             | Connection                | 获取 连接    |
| testConn     | SourceDTO             | Boolean                   | 校验 连接    |
| executeQuery | SourceDTO,SqlQueryDTO | List<Map<String, Object>> | 执行查询     |
| getTableList | SourceDTO,SqlQueryDTO | List<String>              | 获取数据库表名称 |

---
## 五、后续开发计划
1. IClient DTO 优化，为 Kerberos 和 复用 Connection 准备
2. Kerberos 支持，需要支持注解、支持 Start & Close
3. SQLServer 分版本支持，支持 SQLServer 2012、2008、2017&Later
4. Mysql 分版本支持，支持 Mysql 5 及 Mysql 8、TiDB 兼容，使用 Mysql 支持
5. Phoenix 常用版本支持
6. 复用 Connection 支持，需要支持注解、支持 Start & Close
7. 其他非关系型数据库支持、关系型数据库支持、不同版本支持，根据项目需要支持

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
        IClient client = clientCache.getClient(DataSourceType.MySQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:mysql://172.16.8.109:3306/ide")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Connection clientCon = client.getCon(source);
    }
```

2. 直接使用工具封装的方法，具体见第四点

```$java
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    @Test
    public void getMysqlConnection() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.MySQL.name());
        SourceDTO source = new SourceDTO.SourceDTOBuilder()
                .setUrl("jdbc:mysql://172.16.8.109:3306/ide")
                .setUsername("dtstack")
                .setPassword("abc123")
                .builder();
        Boolean isConnect = client.testConn(source);
    }

```
