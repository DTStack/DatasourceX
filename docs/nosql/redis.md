## redis client

### 一、插件包名称
名称：**redis**

### 二、对应数据源sourceDTO及参数说明

[RedisSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/RedisSourceDTO.java)

参数说明：


- **username**
  - 描述：数据源的用户名
  - 必选：否
  - 默认值：无



- **password**
  - 描述：数据源指定用户名的密码
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
  
  

- **redisMode**
  - 描述：Redis 部署模式
  - 必选：否
  - 默认值：Standalone
  


- **master**
  - 描述：如果为 master slave 的则为 master 的地址
  - 必选：否
  - 默认值：无
  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        RedisSourceDTO source = RedisSourceDTO.builder()
                    .hostPort("xxxx:xxxx")
                    .password("xxxx")
                    .schema("1")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- RedisSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 数据预览
入参类型：
- RedisSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.REDIS.getVal());
        client.getPreview(source, SqlQueryDTO.builder().previewNum(5).tableName("loader_test").build());
```
