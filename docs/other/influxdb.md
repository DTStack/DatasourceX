## influxdb client

### 一、插件包名称
名称：**influxdb**

### 二、对应数据源sourceDTO及参数说明

[InfluxDBSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/InfluxDBSourceDTO.java)

参数说明：

- **url**
  - 描述：influxDB数据库连接地址，格式：http://xxxx:xx 或 https://xxxx:xx
  - 必选：是
  - 默认值：无



- **username**
  - 描述：数据源的用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：否
  - 默认值：无



- **database**
  - 描述：数据源指定库
  - 必选：否
  - 默认值：无


#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        InfluxDBSourceDTO SOURCE_DTO = InfluxDBSourceDTO.builder()
                    .url("http://localhost:8086")
                    .username("xxx")
                    .password("xxx")
                    .database("wangchuan_test")
                    .build();;
```

###### 1. 校验数据源连通性
入参类型：
- InfluxDBSourceDTO：数据源连接信息

出参类型：
- Boolean：数据源连接

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 执行查询
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<Map<String, Object>>：执行结果

使用：
```$java
        // 普通查询
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        String sql = "select * from dtstack limit 10";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> mapList = client.executeQuery(sourceDTO, queryDTO);
```

###### 3. 获取所有的表名称，支持模糊查询、条数限制
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(sourceDTO, queryDTO);
```

###### 4. 获取指定库下的的表名称，支持模糊查询、条数限制
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().limit(10).tableNamePattern("xxx").schema("dtstack").build();
        // 也可以放到 InfluxDBSourceDTO 的 database 中
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
```

###### 5. 获取表字段信息，必须指定 database，在sourceDTO的database 或者 SqlQueryDTO 的 schema 中
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
```

###### 6. 数据预览
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").previewNum(200).build();
        List<List<Object>> previewDate = client.getPreview(source, queryDTO);
```

###### 7. 获取所有库
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：库集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        List<String> allDatabases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
```

###### 8. 创建库
入参类型：
- InfluxDBSourceDTO：数据源连接信息
- String：库名
- String：库注释

出参类型：
- Boolean：执行结果

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.INFLUXDB.getVal());
        Boolean check = client.createDatabase(source, "dtstack", "");
```
