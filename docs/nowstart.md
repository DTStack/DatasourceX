## 下载代码

使用git工具把项目clone到本地

```
git clone https://github.com/DTStack/DatasourceX.git
```
## 编译插件

> 第一次编译参考[缺失 jar 处理](../jars/README.md)
```bash
mvn clean install -DskipTests -T 2 -U
```
## 配置pom依赖

项目若想使用数据源插件，需要在项目中依赖 datasourcex 的 client 模块

```$xml
<dependency>
    <groupId>com.dtstack.datasourcex</groupId>
    <artifactId>datasourcex-client</artifactId>
    <version>5.2.1-SNAPSHOT</version>
</dependency>
```

## 配置插件路径

打包完的插件在 datasourcex-plugin/pluginLibs 目录下，需要将该目录放到项目的根路径下，也可以通过 ClientCache.setUserDir("xxx") 重新定义插件包位置;

## 具体使用

每种数据源支持的方法和详细使用请看对应的文档

分为两种方案：

1. 类似于 DriverManger.getConnection 类似，直接使用 Connection 去做二次开发使用
```$Java
    // 不使用插件获取连接的方式
    String url = "jdbc:mysql://xxx/xxx";
    Properties prop = new Properties();
    prop.put("user", "xxxx");
    prop.put("password", "xxxx");
    Class.forName(dataBaseType.getDriverClassName());
    Connection conn = DriverManager.getConnection(url, prop);

    // 插件获取连接的方式
    @Test
    public void getMysqlConnection() throws Exception {
    IClient client = ClientCache.getClient(DataSourceType.MySql5.getVal());
        Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://xxxx")
            .username("xxxx")
            .password("xxxx")
            .poolConfig(PoolConfig.builder().build())
            .build();
        Connection conn = client.getCon(source);
    }
```

2. 直接使用工具封装的方法，具体见各数据源对应的使用文档
```$java
    private static final IClient client = ClientCache.getClient(DataSourceType.MySQL.getVal());

    @Test
    public void testMysqlConnection() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.MySql5.getVal());
        Mysql5SourceDTO source = Mysql5SourceDTO.builder()
            .url("jdbc:mysql://xxxx")
            .username("xxxx")
            .password("xxxx")
            .poolConfig(PoolConfig.builder().build())
            .build();
        Boolean isConnect = client.testConn(source);
    }
```
```$java
    //kafka客户端插件使用
    @Test
    public void getTopicList() throws Exception {
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA_09.getVal());
        KafkaSourceDTO source = KafkaSourceDTO.builder()
                        .url("xxxx")
                        .build();
        List<String> topicList = client.getTopicList(source);
    }
```