## hbase client

### 一、插件包名称
名称：**hbase**

### 二、对应数据源sourceDTO及参数说明

[HbaseSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/HbaseSourceDTO.java)

参数说明：


- **url**
  - 描述：数据源的用户名
  - 必选：是
  - 默认值：无
  
  
  
- **path**
  - 描述：zk 根节点
  - 必选：否
  - 默认值：false
  


- **config**
  - 描述：hbase 配置信息
  - 必选：是
  - 默认值：无  
  

- **others**
  - 描述：其他配置信息
  - 必选：否
  - 默认值：无
  
  

- **kerberosConfig**
  - 描述：kerberos配置信息
  - 必选：否
  - 默认值：无
  


- **poolConfig**
  - 描述：连接池信息
  - 必选：否
  - 默认值：无
  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        HbaseSourceDTO source = HbaseSourceDTO.builder()
                    .url("xxxxx:2181")
                    .path("/hbase2")
                    .poolConfig(PoolConfig.builder().build())
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- HbaseSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取表集合
入参类型：
- HbaseSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<String> tableList = client.getTableList(source, null);
```

###### 3. 获取表字段信息
入参类型：
- HbaseSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<ColumnMetaDTO> metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test").build());
```

###### 4. 自定义查询
入参类型：
- HbaseSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<Map<String, Object>>：查询结果

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        ArrayList<Filter> filters = new ArrayList<>();
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.GREATER, new BinaryComparator("0".getBytes()));
        hbaseRowFilter.setReversed(true);
        filters.add(hbaseRowFilter);
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").hbaseFilter(filters).build();
        List<Map<String, Object>> result = client.executeQuery(source, sqlQueryDTO);
```

###### 5. 数据预览
入参类型：
- HbaseSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HBASE.getVal());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("loader_test").previewNum(2).build());
```

##### IHbase客户端使用

构造sourceDTO

```$java
        HbaseSourceDTO source = HbaseSourceDTO.builder()
                    .url("xxxxx:2181")
                    .path("/hbase2")
                    .poolConfig(PoolConfig.builder().build())
                    .build();
```

###### 1. 检测namespace是否存在
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：namespace

出参类型：
- Boolean：namespace是否存在

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        Boolean check = hbaseClient.isDbExists(source, "default");
```

###### 2. 创建表
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：tableName
- String[]：列族信息

出参类型：
- Boolean：namespace是否存在

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        Boolean check = hbaseClient.createHbaseTable(source, "loader_test_2", new String[]{"info1", "info2"});
```

###### 3. 根据rowKey正则获取对应的rowKey列表
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：tableName
- String：正则表达式

出参类型：
- List<String>：查询信息

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        List<String> list = hbaseClient.scanByRegex(source, "loader_test_2", ".*_loader");
```

###### 4. 插入一条数据
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：tableName
- String：rowKey
- String：列族
- String：列名
- String：数据信息

出参类型：
- Boolean：插入结果

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        Boolean check = hbaseClient.putRow(source, "loader_test_2", "1002", "info1", "name", "wangchuan");
```

###### 5. 获取指定数据
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：表名
- String：rowKey
- String：列族
- String：列名

出参类型：
- String：查询数据

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        String row = hbaseClient.getRow(source, "loader_test_2", "1003", "info2", "name");
```

###### 6. 删除指定rowKey数据
入参类型：
- HbaseSourceDTO：数据源连接信息
- String：表名
- String：列族
- String：列名
- List<String>：rowKey集合

出参类型：
- String：查询数据

使用：
```$java
        IHbase hbaseClient = ClientCache.getHbase(DataSourceType.HBASE.getVal());
        Boolean check = hbaseClient.deleteByRowKey(source, "loader_test_2", "info1", "name", Lists.newArrayList("1001", "1002"));
```

