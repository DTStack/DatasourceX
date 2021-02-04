## emq client

### 一、插件包名称
名称：**emq**

### 二、对应数据源sourceDTO及参数说明

[EMQSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/EMQSourceDTO.java)

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
  - 描述：连接地址
  - 必选：否
  - 默认值：无
  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        EMQSourceDTO source = EMQSourceDTO.builder()
                    .username("xxx")
                    .password("xxx")
                    .url("tcp://xxx:1883")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- EMQSourceDTO：数据源连接信息

出参类型：
- Boolean：数据源连接

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.EMQ.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```
