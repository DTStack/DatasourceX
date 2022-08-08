# 数据源开启连接池

关系型数据库连接池使用Hikari实现，为需要开启线程池的每个数据源缓存一个线程池
非关系型数据库连接池利用common-pool实现或者数据源本身client支持连接池，那么就缓存该client客户端
### 1. common-loader支持开启连接池的插件：(如果数据源开启kerberos认证，连接池不会生效)

- ClickHouse
- Oracle
- MySQL
- MySQL8
- Kingbase8
- Oracle
- SQLServer
- PostgreSQL
- Phoenix4
- Phoenix5
- Libra
- DB2
- DMDB
- ImpalaSql
- TiDB
- Kylin
- Vertica
- MaxCompute
- ElasticSearch
- MongoDB
- hbase

### 2. 开启方法

开启池化需要在对应的SourceDTO传入poolConfig对象，不传不开启（默认不开启）
1. poolConfig中属性含义：

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| connectionTimeout | 等待连接池分配连接的最大时长（毫秒），超过这个时长还没可用的连接则发生SQLException | 否 | 30 * 1000 | Long |
| idleTimeout | 控制允许连接在池中闲置的最长时间 (毫秒)，此设置仅适用于 minimumIdle 设置为小于 maximumPoolSize 的情况 | 否 | 10 * 60 * 1000 | Long |
| maxLifetime | 一个连接的生命时长（毫秒），超时而且没被使用则被释放 | 否 | 30 * 60 * 1000 | Long |
| maximumPoolSize | 连接池中允许的最大连接数 (包括空闲和正在使用的连接) | 否 | 10 | Integer |
| minimumIdle | 池中维护的最小空闲连接数，小于 0 则会重置为最大连接数 | 否 | 5 | Integer |
| readOnly | 设置连接只读 | 否 | false | Boolean |

2. 具体使用：

```$java
    @Test
    public void getCon() throws Exception {
        PoolConfig poolConfig = PoolConfig.builder()
                .maximumPoolSize(20)
                .maxLifetime(20L)
                .idleTimeout(15L)
                .minimumIdle(5)
                .readOnly(true)
                .connectionTimeout(30L).build();
        Mysql5SourceDTO source = Mysql5SourceDTO.builder()
                .url("jdbc:mysql://xxxxx")
                .username("xxx")
                .password("xxx")
                .poolConfig(poolConfig)
                .build();
        IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());
        Connection con = client.getCon(source);
        con.close();
    }
```
