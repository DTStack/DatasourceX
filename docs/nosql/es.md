## es client

### 一、插件包名称
名称：**es**

### 二、对应数据源sourceDTO及参数说明

[ESSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/ESSourceDTO.java)

参数说明：


- **username**
  - 描述：数据源的用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：否
  - 默认值：无



- **url**
  - 描述：es连接地址
  - 必选：否
  - 默认值：无
  
  
  
- **poolConfig**
  - 描述：连接池配置信息，如果传入则认为开启连接池
  - 必选：否
  - 默认值：无
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        ESSourceDTO source = ESSourceDTO.builder()
                    .username("xxx")
                    .password("xxx")
                    .url("xxxx:9200")
                    .poolConfig(new PoolConfig())
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- ESSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取当前db下的表(就是获取es某一索引下所有type)
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        List<String> tableList = client.getTableList(source, SqlQueryDTO.builder().tableName("commodity").build());
```

###### 3. 获取所有db(就是获取es所有索引)
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：索引集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        List<String> databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
```

###### 4. 数据预览
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        List<List<Object>> viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("commodity").previewNum(5).build());
```

###### 5. 获取表字段信息
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("commodity").build());
```


###### 6. 执行自定义查询
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<Map<String, Object>>：自定义查询信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        List<Map<String, Object>> list = client.executeQuery(source, SqlQueryDTO.builder().sql("{\"query\": {\"match_all\": {} }}").tableName("commodity").build());
```

###### 7. 执行无需结果集的自定义语句
入参类型：
- ESSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- Boolean：执行结果

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.es6.getVal());
        String sql = "{\"name\": \"小黄\", \"age\": 18,\"sex\": \"不详\",\"extraAttr_0_5_3\":{\"attributeValue\":\"2020-09-17 23:37:16\"}}";
        String tableName = "commodity/_doc/3";
        client.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().sql(sql).tableName(tableName).esCommandType(EsCommandType.INSERT.getType()).build());
```
