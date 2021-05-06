## s3 client

### 一、插件包名称
名称：**aws_s3**

### 二、对应数据源sourceDTO及参数说明

[AwsS3SourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/AwsS3SourceDTO.java)

参数说明：


- **accessKey**
  - 描述：aws s3 文件访问密钥
  - 必选：是
  - 默认值：无



- **secretKey**
  - 描述：aws s3 密钥
  - 必选：是
  - 默认值：无



- **region**
  - 描述：桶所在区
  - 必选：否
  - 默认值：cn-north-1
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        AwsS3SourceDTO sourceDTO = AwsS3SourceDTO.builder()
                    .accessKey("******")
                    .secretKey("******")
                    .region("cn-north-1")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- AwsS3SourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.AWS_S3.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取所有的桶列表
入参类型：
- AwsS3SourceDTO：数据源连接信息
- SqlQueryDTO   ：查询信息

出参类型：
- List<String>：该region下的bucket(桶)列表

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.AWS_S3.getVal());
        List<String> buckets = CLIENT.getTableList(SOURCE_DTO, SqlQueryDTO.builder().build());
        // buckets即为桶集合
```
