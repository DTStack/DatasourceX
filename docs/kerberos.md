# 数据源开启Kerberos安全认证

common-loader支持开启kerberos认证的插件：
- hive1.X
- hive2.X
- spark
- HDFS
- hbase
- impala
- kudu
- phoenix5
- kafka
- solr

### Kerberos证书认证方式

目前支持两种方式，一种是指定zip包路径，另一种直接在sourceDTO中指定kerberos认证参数
#### 1. 解析zip包来进行kerberos认证：

zip包中必须要有的文件：
1. 含有可用principal的keytab文件，文件以.keytab结尾
2. krb5.conf，用于指定认证的kdc、releam等信息，文件以.conf结尾

注意：
1. zip文件中不能包含目录
2. 文件中可包含xml配置文件，会解析成map键值对

具体使用：

```$Java
    @Test
    public void getHiveConnection() throws Exception {
        // 获取kerberos认证的客户端
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HIVE.getVal());
        // 处理 Kerberos，解压缩、解析xml文件，和定位 keytab 和 krb5.conf 文件路径
        // 返回的map集合参数中有principalFile(keytab文件相对路径)，java.security.krb5.conf(krb5.conf文件相对路径)和xml文件中对应配置参数
        // localKerberosPath为解压kerberos文件的路径
        Map<String, Object> kerberosConfig = kerberos.parseKerberosFromUpload(zipLos, localKerberosPath);
        // 调用Client 方法前调用
        // 1. 会替换相对路径到绝对路径，目前支持的是存在一个或者一个 / 都不存在的情况
        // 2. 会增加或者修改一些 Principal 参数
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
        // kerberosConfig 就是 KerberosConfig 参数拿来个 Client 使用
        // 如果kerberosConfig中没有指定认证的principal，则会使用keytab文件中的第一个principal账号来进行认证
        // 可以调用如下方法选择principal
        List<String> principals = kerberos.getPrincipals(kerberosConfig);
        KerberosConfig.put("principal", "xxx");//此处的xxx可以从principals中获取
        HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://xxx/xxx;principal=xxx")
            .defaultFS("xxx")
            .kerberosConfig(KerberosConfig)
            .build();
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Connection conn = client.getCon(source);
    }
```
#### 2. 在sourceDTO中指定kerberos认证参数：
需要在数据源对应的sourceDTO中的kerberosConfig中指定principal认证参数，kerberosConfig变量为map集合

| 名称 | 说明 | 是否必填 | 默认值 | 参数类型 |
| --- | --- | --- | --- | --- |
| principal | kerberos认证账号 | 否 | 无 | String |
| principalFile | keytab文件绝对路径 | 是 | 无 | String |
| java.security.krb5.conf | krb5文件绝对路径 | 是 | 无 | String |
| ~~keytabPath~~ | keytab文件绝对路径，兼容历史数据，不需要传该值 | 否 | 无 | String |
具体使用：

```$Java
    @Test
    public void getHiveConnection() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put("principal", "xxx");
        kerberosConfig.put("principalFile", "xxx");
        kerberosConfig.put("java.security.krb5.conf", "xxx");
        HiveSourceDTO source = HiveSourceDTO.builder()
            .url("jdbc:hive2://xxx/xxx;principal=xxx")
            .defaultFS("xxx")
            .kerberosConfig(KerberosConfig)
            .build();
        IClient client = ClientCache.getClient(DataSourceType.HIVE.getVal());
        Connection conn = client.getCon(source);
    }
```