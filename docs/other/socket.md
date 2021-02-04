## socket client

### 一、插件包名称
名称：**socket**

### 二、对应数据源sourceDTO及参数说明

[SocketSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/SocketSourceDTO.java)

参数说明：

- **hostPort**
  - 描述：host 和 端口号
  - 必选：是
  - 默认值：无  
  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        SocketSourceDTO source = SocketSourceDTO.builder()
                    .hostPort("xxxx:xxxx")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- SocketSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```
