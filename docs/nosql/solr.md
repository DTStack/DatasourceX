## solr client

### 一、插件包名称
名称：**solr**

### 二、对应数据源sourceDTO及参数说明

[SolrSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/SolrSourceDTO.java)

参数说明：


- **zkHost**
    - 描述：zk地址
    - 必选：是
    - 默认值：无



- **chroot**
    - 描述：chroot
    - 必选：是
    - 默认值：无

- **poolConfig**
    - 描述：连接池配置信息，如果传入则认为开启连接池
    - 必选：否
    - 默认值：无

#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
         SolrSourceDTO source = SolrSourceDTO.builder()
            .zkHost("worker:2181")
            .chroot("/solr")
            .poolConfig(new PoolConfig())
            .build();
```

###### 1. 校验数据源连通性
入参类型：
- SolrSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取当前db下的表(就是获取solr所有的collection)
入参类型：
- SolrSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());
        List<String> tableList = client.getTableList(source, SqlQueryDTO.builder().tableName("commodity").build());
```

###### 3. 数据预览
入参类型：
- SolrSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());
        List<List<Object>> viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("qianyi_test").previewNum(5).build());
```

###### 4. 获取表字段信息
入参类型：
- SolrSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("qianyi_test").build());
```
