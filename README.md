# 数栈 SPI

## 介绍
模拟 Driver 通过不同的 classloader 加载，避免类名冲突导致加载不上的问题。

## 使用配置

> 将打包完的 pluginLibs 下需要的插件放到对应 jar 包的 root 目录，并已pluginLibs 为外目录，例如：/home/admin/app/dt-center-ide/pluginLibs/clickhouse/dtClickhouse.jar

## 已支持数据源（具体版本后续补充）

### 关系型数据库
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

### 非关系型数据库
* Redis
* MongoDB

## 已支持方法

| 方法           | 入参                    | 出参                        | 备注       |
|--------------|-----------------------|---------------------------|----------|
| getConn      | SourceDTO             | Connection                | 获取 连接    |
| testConn     | SourceDTO             | Boolean                   | 校验 连接    |
| executeQuery | SourceDTO,SqlQueryDTO | List<Map<String, Object>> | 执行查询     |
| getTableList | SourceDTO,SqlQueryDTO | List<String>              | 获取数据库表名称 |

## 开发步骤说明
### 增加支持方法
1. com.dtstack.dtcenter.loader.client.IClient 中增加对应的方法
2. com.dtstack.dtcenter.loader.client.sql.DataSourceClientProxy 中增加代理实现
3. 对应的抽象类和具体实现类补充对应的方法实现

### 增加支持的数据源
1. 在对应的关系型数据库模块或者非关系型模块增加子模块并按照其他模块修改对应的pom
2. 继承对应抽象类并重写对应方法
3. 在Resources/META-INF/services 下增加文件com.dtstack.dtcenter.loader.client.IClient，并在里面补充实现类的引用地址：例如：com.dtstack.dtcenter.common.loader.rdbms.db2.Db2Client