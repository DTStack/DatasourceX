## ftp client

### 一、插件包名称
名称：**ftp**

### 二、对应数据源sourceDTO及参数说明

[FtpSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/FtpSourceDTO.java)

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
  
  
  
- **url**
  - 描述：ftp路径
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
  
  

- **protocol**
  - 描述：协议
  - 必选：否
  - 默认值：无
  


- **auth**
  - 描述：认证方式
  - 必选：否
  - 默认值：无
  
  
  
- **path**
  - 描述：FTP rsa 路径
  - 必选：否
  - 默认值：无
  
  
  
- **connectMode**
  - 描述：连接方式
  - 必选：否
  - 默认值：无
  
  
#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        FtpSourceDTO source = FtpSourceDTO.builder()
                    .url("0.0.0.0")
                    .hostPort("22")
                    .username("root")
                    .password("xxxx")
                    .protocol("sftp")
                    .build();
```

###### 1. 校验数据源连通性
入参类型：
- FtpSourceDTO：数据源连接信息

出参类型：
- Boolean：是否连通

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.FTP.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```