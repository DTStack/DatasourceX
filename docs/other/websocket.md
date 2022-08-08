## websocket client

### 一、插件包名称
名称：**websocket**

### 二、对应数据源sourceDTO及参数说明

[WebSocketSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/WebSocketSourceDTO.java)

参数说明：

- **url**
  - 描述：websocket url地址
  - 必选：是
  - 默认值：无  



- **authParams**
  - 描述：鉴权参数
  - 必选：是
  - 默认值：无  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        HashMap<String, String> authMap = new HashMap<>();
        map.put("xxx", "xxx");
        WebSocketSourceDTO sourceDTO = WebSocketSourceDTO.builder().url("ws://xxxx:xxxx/xxxx").authParams(authMap).build();
```

###### 1. 校验数据源连通性
入参类型：
- WebSocketSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.WEB_SOCKET.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```
