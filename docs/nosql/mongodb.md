## redis client

### 一、插件包名称
名称：**mongo**

### 二、对应数据源sourceDTO及参数说明

[MongoSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/MongoSourceDTO.java)

参数说明：


- **username**
  - 描述：数据源的用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
  - 必选：否
  - 默认值：无



- **sourceType**
  - 描述：数据源类型
  - 必选：否
  - 默认值：无
  
  
  
- **isCache**
  - 描述：是否缓存
  - 必选：否
  - 默认值：false
  


- **hostPort**
  - 描述：host 和 端口号
  - 必选：是
  - 默认值：无  
  

- **schema**
  - 描述：redis db
  - 必选：否
  - 默认值：0
  
  

- **poolConfig**
  - 描述：连接池配置信息，如果传入则认为开启连接池
  - 必选：否
  - 默认值：无
  

#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        MongoSourceDTO source = MongoSourceDTO.builder()
                    .hostPort("xxxxx:xxxx")
                    .schema("admin")
                    .poolConfig(new PoolConfig())
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- MongoSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取当前db下的表
入参类型：
- RedisSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        List<String> tableList = client.getTableList(source, SqlQueryDTO.builder().build());
```

###### 3. 获取所有schema
入参类型：
- RedisSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：schema集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        List list = client.getAllDatabases(source, queryDTO);
```

###### 4. 数据预览
入参类型：
- MongoSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("user").build();
        List<List<Object>> preview = client.getPreview(source, queryDTO);
```

###### 5. 执行自定义查询
入参类型：
- MongoSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.MONGODB.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("db.user.find({});").startRow(1).limit(5).build();
        List<Map<String, Object>> result = client.executeQuery(source, queryDTO);
```