## hive/hive1/spark client

### 一、插件包名称
名称：**hive/hive1/spark**

### 二、对应数据源sourceDTO及参数说明

hive3对应sourceDTO：[Hive3SourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Hive3SourceDTO.java)

hive2对应sourceDTO：[HiveSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/HiveSourceDTO.java)

hive1对应sourceDTO：[Hive1SourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/Hive1SourceDTO.java)

spark对应sourceDTO：[SparkSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/SparkSourceDTO.java)

参数说明：

- **url**
  - 描述：数据源对应的jdbc连接字符串
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



- **schema**
  - 描述：数据源指定库
  - 必选：否
  - 默认值：无



- **connection**
  - 描述：数据源指定connection
  - 必选：否
  - 默认值：无



- **defaultFS**
  - 描述：hdfs defaultFs
  - 必选：否
  - 默认值：无



- **config**
  - 描述：hadoop 高可用配置信息，json格式
  - 必选：否
  - 默认值：无



- **poolConfig**
  - 描述：数据源池化参数，详见[连接池使用](../connectionPool.md)
  - 必选：否
  - 默认值：无

#### 三、支持的方发及使用demo

##### IClient客户端使用

构造数据源对应的sourceDTO，以hive2 为例

```$java
        HiveSourceDTO sourceDTO = HiveSourceDTO.builder()
                    .url("jdbc:hive2://xxxx")
                    .username("xxxx")
                    .password("xxxx")
                    .defaultFS("hdfs://ns1")
                    .config("{\n" +
                            "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                            "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                            "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                            "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                            "    \"dfs.nameservices\": \"ns1\"\n" +
                            "}")
                    .build();
```

###### 1. 获取连接
入参类型：
- HiveSourceDTO：数据源连接信息

出参类型：
- Connection：数据源连接

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Connection conn = client.getCon(sourceDTO);
```

###### 2. 校验数据源连通性
入参类型：
- HiveSourceDTO：数据源连接信息

出参类型：
- Connection：数据源连接

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 3. 执行查询
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<Map<String, Object>>：执行结果

使用：
```$java
        // 普通查询
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        String sql = "select * from dtstack limit 10";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        List<Map<String, Object>> mapList = client.executeQuery(sourceDTO, queryDTO);
```
```$java
        // 预编译查询
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        String sql = "select * from dtstack limit 10 where id > ? and id < ? ";
        List<Object> preFields = new ArrayList<>();
        preFields.add(2);
        preFields.add(5);
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).preFields(preFields).build();
        List<Map<String, Object>> mapList = client.executeQuery(sourceDTO, queryDTO);
```

###### 4. 执行不需要结果集的sql
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- Boolean：执行成功与否

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        String sql = "create table if not exists dtstack (id int)";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(sql).build();
        client.executeSqlWithoutResultSet(sourceDTO, queryDTO);
```

###### 5. 获取所有的表名称
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(sourceDTO, queryDTO);
```

###### 6. 获取指定库下的的表名称，支持正则匹配、条数限制
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().limit(10).tableNamePattern("xxx").schema("dtstack").build();
        List<String> tableList = client.getTableListBySchema(source, queryDTO);
```

###### 7. 获取表字段 Java 类的标准名称
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表字段 Java 类的标准名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
```

###### 8. 获取表字段信息
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
```

###### 9. 获取表注释
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- String：表字段 Java 类的标准名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        String comment = client.getTableMetaComment(source, queryDTO);
```

###### 10. 数据预览
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").previewNum(200).build();
        List<List<Object>> previewDate = client.getPreview(source, queryDTO);
```

###### 11. 获取所有库
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：库集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        List<String> allDatabases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
```

###### 12. 获取建表sql
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- String：建表sql

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        String createTableSql = client.getCreateTableSql(source, queryDTO);
```

###### 13. 获取当前使用的数据库
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- String：正在使用的数据库

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        String currentDatabase = client.getCurrentDatabase(source);
```

###### 14. 根据sql获取字段信息
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from dtstack").build();
        List<ColumnMetaDTO> result = client.getColumnMetaDataWithSql(source, queryDTO);
```

###### 15. 获取HIVE数据下载器
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- IDownloader：表数据下载器

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from nanqi").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        downloader.configure();
        List<String> metaInfo = downloader.getMetaInfo();
        System.out.println(metaInfo);
        while (!downloader.reachedEnd()){
            List<List<String>> result = (List<List<String>>)downloader.readNext();
            for (List<String> row : result){
                System.out.println(row);
            }
        }
```

###### 16. 创建库
入参类型：
- HiveSourceDTO：数据源连接信息
- String：库名
- String：库注释

出参类型：
- Boolean：执行结果

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Boolean check = client.createDatabase(source, "dtstack", "comment");
```

###### 17. 判断库是否存在
入参类型：
- HiveSourceDTO：数据源连接信息
- String：库名

出参类型：
- Boolean：是否存在

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Boolean check = client.isDatabaseExists(source, "dtstack");
```

###### 18. 判断表是否存在指定库下
入参类型：
- HiveSourceDTO：数据源连接信息
- String：库名

出参类型：
- Boolean：是否存在

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Boolean check = client.isTableExistsInDatabase(source, "dtstack", "db");
```

###### 19. 获取表详细信息
入参类型：
- HiveSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- Table：[Table](/core/src/main/java/com/dtstack/dtcenter/loader/dto/Table.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Table table = client.getTable(source, SqlQueryDTO.builder().tableName("xxx").build());
```

