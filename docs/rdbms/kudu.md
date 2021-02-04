## kudu client

### 一、插件包名称
名称：**kudu**

### 二、对应数据源sourceDTO及参数说明

[KuduSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/KuduSourceDTO.java)

参数说明：

- **url**
  - 描述：kudu数据库的jdbc连接字符串
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



- **poolConfig**
  - 描述：数据源池化参数，详见[连接池使用](../connectionPool.md)
  - 必选：否
  - 默认值：无

#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        KuduSourceDTO sourceDTO = KuduSourceDTO.builder()
                    .url("jdbc:Kudu://xxxx")
                    .username("xxxx")
                    .password("xxxx")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- KuduSourceDTO：数据源连接信息

出参类型：
- Boolean：是否成功

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取所有的表名称
入参类型：
- KuduSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<String>：表名称集合

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(sourceDTO, queryDTO);
```

###### 3. 获取表字段信息
入参类型：
- KuduSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<ColumnMetaDTO>：表字段信息集合[ColumnMetaDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/ColumnMetaDTO.java)

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
```

###### 4. 数据预览
入参类型：
- KuduSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：
- List<List<Object>>：预览数据信息

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.Kudu.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("dtstack").previewNum(200).build();
        List<List<Object>> previewDate = client.getPreview(source, queryDTO);
```
