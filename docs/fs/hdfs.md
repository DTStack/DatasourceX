## hdfs client

### 一、插件包名称
名称：**hdfs**

### 二、对应数据源sourceDTO及参数说明

[HdfsSourceDTO](../../datasourcex-common/src/main/java/com/dtstack/dtcenter/loader/dto/source/HdfsSourceDTO.java)

参数说明：


- defaultFS
  - 描述：hdfs defaultFs
  - 必选：是
  - 默认值：""



- **config**
    - 描述：配置信息
    - 必选：否
    - 默认值：无

#### 三、支持的方法及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
         HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();
```

###### 1. 测试数据源连通性
入参类型：
- HdfsSourceDTO：数据源连接信息

出参类型：
- Boolean：连接成功与否

使用：
```$java
        IClient client = ClientCache.getClient(DataSourceType.HDFS.getVal());
        Boolean conn = client.testCon(sourceDTO);
```

##### IHdfsFile客户端使用

构造sourceDTO

```$java
         HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();
```


###### 1. 获取文件状态
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 文件路径

出参类型：
- FileStatus：文件状态

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        FileStatus fileStatus = client.getStatus(source, "/tmp");
```

###### 2. 日志下载器
入参类型：
- HdfsSourceDTO：数据源连接信息
- SqlQueryDTO : 查询信息

出参类型：
- IDownloader：日志下载器

使用：
```$java
        HashMap<String, Object> yarnConf = Maps.newHashMap();
        yarnConf.put("yarn.log-aggregation.file-formats", "IFile,TFile");
        yarnConf.put("dfs.namenode.kerberos.principal", "hdfs/_HOST@DTSTACK.COM");
        source.setYarnConf(yarnConf);
        source.setReadLimit(1024 * 1024 * 30);
        source.setAppIdStr("application_1610953567506_0002");
        
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        IDownloader logDownloader = client.getLogDownloader(source, queryDTO);
```

###### 3. 文件下载器
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 文件路径

出参类型：
- IDownloader：文件下载器

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        IDownloader iDownloader = client.getFileDownloader(source, "/path");
```

###### 4. 从 HDFS 上下载文件或文件夹到本地
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : hdfs路径
- String : 本地路径

出参类型：
- Boolean：下载状态

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        String localKerberosPath = HdfsFileTest.class.getResource("/hdfs-file").getPath();
        Boolean hasDownload = client.downloadFileFromHdfs(source, "/tmp/test.txt", localKerberosPath);
```

###### 5. 上传文件到 HDFS
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 本地路径
- String : hdfs路径

出参类型：
- Boolean：上传状态

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        String localKerberosPath = HdfsFileTest.class.getResource("/hdfs-file").getPath();
        Boolean hasUpload = client.uploadLocalFileToHdfs(source, localKerberosPath + "/test.txt", "/tmp");
```

###### 6. 上传字节流到 HDFS
入参类型：
- HdfsSourceDTO：数据源连接信息
- byte[]: 字节数组
- String：hdfs路径

出参类型：
- Boolean：上传状态

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean hasUpload = client.uploadInputStreamToHdfs(source, "test".getBytes(), "/tmp/test.txt");
```

###### 7. 创建 HDFS 路径
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : hdfs路径
- Short ： 权限

出参类型：
- Boolean：创建状态

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean hasCreated = client.createDir(source, "/tmp/test", null);
```

###### 8. 路径文件是否存在
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : hdfs路径

出参类型：
- Boolean：是否存在

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean isExist = client.isFileExist(source, "/tmp/test.txt");
```

###### 9. 文件检测并删除
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : hdfs路径

出参类型：
- Boolean：删除结果

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean isDeleted = client.checkAndDelete(source, "/tmp/test.txt");
```
###### 10. 直接删除目标路径文件
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 目标路径
- Boolean: 是否递归删除

出参类型：
- Boolean：删除结果

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean isDeleted = delete(source, "/tmp", true);
```

###### 11.  获取路径文件大小
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 目标路径

出参类型：
- Long：文件大小

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Long size = client.getDirSize(source, "/tmp/test.txt");
```

###### 12.  删除文件
入参类型：
- HdfsSourceDTO：数据源连接信息
-  List<String> : 文件路径列表

出参类型：
- Boolean：删除结果

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean isDeleted = client.deleteFiles(source, Lists.newArrayList("/tmp/test1.txt"));
```

###### 13.  路径目录是否存在
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 目标路径

出参类型：
- Boolean：是否存在

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean isExist = client.isDirExist(source, "/tmp/test.txt");
```

###### 14.  设置路径权限
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 目标路径
- String : 文件权限

出参类型：
- Boolean：是否设置成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.setPermission(source, "/tmp/test.txt", "ugoa=rwx");
```

###### 15.  文件重命名
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 原路径
- String : 目标路径

出参类型：
- Boolean：设置是否成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.rename(source, "/tmp/test.txt", "/tmp/test1.txt");
```

###### 16.  复制文件
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 原路径
- String : 目标路径
- Boolean: 是否覆盖

出参类型：
- Boolean：是否成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.copyFile(source, "/tmp/test.txt", "/tmp/test.txt", true);
```

###### 17.  复制文件夹
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 原路径
- String : 目标路径

出参类型：
- Boolean：是否成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.copyDirector(source, "/tmp/test.txt", "/tmp/test.txt");
```

###### 18.  合并小文件
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 合并路径
- String : 目标路径 
- FileFormat: 文件类型 ：text、orc、parquet {@link com.dtstack.dtcenter.loader.enums.FileFormat}
- Long: 合并后的文件大小
- Long: 小文件的最大值，超过此阈值的小文件不会合并

出参类型：
- Boolean：合并成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.fileMerge(source, "/tmp", "/tmp", FileFormat.TEXT.getVal(), 1000, 1000);
```

###### 19.  获取目录下所有文件
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 文件路径

出参类型：
- List<String>：文件列表

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        List<String> list = client.listAllFilePath(source, "/tmp");
```

###### 20.  获取目录下所有文件的属性集
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 文件路径
- Boolean: 是否递归

出参类型：
- List<FileStatus>：文件属性列表

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        List<FileStatus> result = client.listAllFiles(source, "/tmp/hive_test", true);
```

###### 21.  从hdfs copy文件到本地
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 原路径
- String : 目标路径

出参类型：
- Boolean：是否成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.copyToLocal(source, "/tmp/test.txt", "/tmp/test.txt");
```

###### 22.  从本地copy到hdfs
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 原路径
- String : 目标路径
- Boolean: 是否重写

出参类型：
- Boolean：是否成功

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        Boolean successed = client.copyFromLocal(source, "/tmp/test.txt", true);
```

###### 23.  根据文件格式获取对应的downlaoder
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : 文件路径
- List<String> : 列名称
- String: 文件分隔符
- String: 文件类型

出参类型：
- IDownloader: 文件下载器

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        IDownloader iDownloader = client.getDownloaderByFormat(source, "/tmp/textfile", Lists.newArrayList("id", "name"),",", FileFormat.TEXT.getVal());
```

###### 24.  获取hdfs上存储文件的字段信息
入参类型：
- HdfsSourceDTO：数据源连接信息
- SqlQueryDTO : 查询信息
- String: 文件类型

出参类型：
- Boolean：是否存在

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        List<ColumnMetaDTO> columnList = client.getColumnList(source, SqlQueryDTO.builder().tableName("/tmp/orcfile").build(), FileFormat.ORC.getVal());
```

###### 25.  按位置写入
入参类型：
- HdfsSourceDTO：数据源连接信息
- HdfsWriterDTO : hdfs写入信息

出参类型：
- int：写入行数

使用：
```$java
        HdfsWriterDTO writerDTO = new HdfsWriterDTO();
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setHdfsDirPath("/tmp/hive_test/");
        writerDTO.setFromFileName(localFilePath + "/textfile");
        writerDTO.setFromLineDelimiter(",");
        writerDTO.setToLineDelimiter(",");
        writerDTO.setOriCharSet("UTF-8");
        writerDTO.setStartLine(1);
        
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        int cursor = client.writeByPos(source, writerDTO);
```

###### 26.  从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
入参类型：
- HdfsSourceDTO：数据源连接信息
- HdfsWriterDTO : hdfs写入信息

出参类型：
- int：写入行数

使用：
```$java
        HdfsWriterDTO writerDTO = new HdfsWriterDTO();
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setHdfsDirPath("/tmp/hive_test/");
        writerDTO.setFromFileName(localFilePath + "/textfile");
        writerDTO.setFromLineDelimiter(",");
        writerDTO.setToLineDelimiter(",");
        writerDTO.setOriCharSet("UTF-8");
        writerDTO.setStartLine(1);
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        int cursor = client.writeByName(source, writerDTO);
```

###### 27. 批量统计文件夹内容摘要，包括文件的数量，文件夹的数量，文件变动时间，以及这个文件夹的占用存储等内容
入参类型：
- HdfsSourceDTO：数据源连接信息
- List<String> : hdfs 文件路径列表

出参类型：
-  List<HDFSContentSummary>：文件摘要信息列表

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        List<HDFSContentSummary> list = getContentSummary (source,  Lists.newArrayList("/tmp/test.txt"));
```

###### 28. 统计文件夹内容摘要，包括文件的数量，文件夹的数量，文件变动时间，以及这个文件夹的占用存储等内容
入参类型：
- HdfsSourceDTO：数据源连接信息
- String : hdfs上文件路径

出参类型：
- HDFSContentSummary：文件摘要信息

使用：
```$java
        IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        HDFSContentSummary hdfsContentSummary = getContentSummary (source, "/tmp/test.txt")
```