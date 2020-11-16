package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSImportColumn;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/8/19
 * @Description：HDFS 文件系统测试
 */
public class HdfsFileTest {
    private IHdfsFile client;

    private String localKerberosPath = HdfsFileTest.class.getResource("/hdfs-file").getPath();

    private HdfsWriterDTO writerDTO;

    private HdfsSourceDTO source = HdfsSourceDTO.builder()
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

    private HiveSourceDTO hiveSource = HiveSourceDTO.builder()
            .url("jdbc:hive2://kudu1:10000/dev")
            .schema("dev")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    /**
     * 参数准备
     */
    @Before
    public void setUp () throws Exception{
        System.setProperty("HADOOP_USER_NAME", "root");
        client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());
        client.checkAndDelete(source, "/tmp/hive_test");
        // 创建parquet格式的hive外部表，并插入数据
        IClient hiveClient = ClientCache.getClient(DataSourceType.HIVE.getVal());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("drop table if exists parquetTable ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("create external table if not exists parquetTable(id int,name string) stored as parquet location '/tmp/hive_test' ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("insert into parquetTable values(1,'wangchuan') ").build());
        // 创建text格式","分隔的hive外部表，并插入数据
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("drop table if exists textTable ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("create external table if not exists textTable(id int,name string) row format delimited fields terminated by ',' stored as textfile location '/tmp/hive_test' ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("insert into textTable values(1,'wangchuan') ").build());
        // 创建orc格式的hive外部表，并插入数据
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("drop table if exists orcTable ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("create external table if not exists orcTable(id int,name string) stored as orc location '/tmp/hive_test' ").build());
        hiveClient.executeSqlWithoutResultSet(hiveSource, SqlQueryDTO.builder().sql("insert into orcTable values(1,'wangchuan') ").build());

        writerDTO = new HdfsWriterDTO();
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setHdfsDirPath("/tmp/hive_test/");
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        writerDTO.setFromLineDelimiter(",");
        writerDTO.setToLineDelimiter(",");
        writerDTO.setOriCharSet("UTF-8");
        writerDTO.setStartLine(1);
        HDFSImportColumn column1 = new HDFSImportColumn();
        column1.setKey("id");
        HDFSImportColumn column2 = new HDFSImportColumn();
        column2.setKey("name");
        writerDTO.setKeyList(Lists.newArrayList(column1, column2));
        ColumnMetaDTO metaDTO1 = new ColumnMetaDTO();
        metaDTO1.setKey("id");
        metaDTO1.setType("int");
        ColumnMetaDTO metaDTO2 = new ColumnMetaDTO();
        metaDTO2.setKey("name");
        metaDTO2.setType("string");
        writerDTO.setColumnsList(Lists.newArrayList(metaDTO1, metaDTO2));
    }

    /**
     * 测试hdfs连通性
     * @throws Exception
     */
    @Test
    public void testConn() throws Exception{
        IClient client = ClientCache.getClient(DataSourceType.HDFS.getVal());
        assert  client.testCon(source);
    }

    /**
     * 获取文件状态
     * @throws Exception
     */
    @Test
    public void getStatus () throws Exception {
        FileStatus fileStatus = client.getStatus(source, "/tmp");
        Assert.assertNotNull(fileStatus);
    }

    /**
     * 下载hdfs文件到本地
     */
    @Test
    public void downloadFileFromHdfs() throws Exception {
        client.downloadFileFromHdfs(source, "/tmp/test.txt", localKerberosPath);
    }

    /**
     * 上传本地文件到hdfs
     */
    @Test
    public void uploadLocalFileToHdfs() throws Exception{
        assert client.uploadLocalFileToHdfs(source, localKerberosPath + "/test.txt", "/tmp");
    }

    /**
     * 上传字节流文件到hdfs
     */
    @Test
    public void uploadInputStreamToHdfs() throws Exception{
        assert client.uploadInputStreamToHdfs(source, "test".getBytes(), "/tmp/test.txt");
    }

    /**
     * 在hdfs文件系统创建文件夹
     */
    @Test
    public void createDir() throws Exception {
        client.checkAndDelete(source, "/tmp/test");
        assert client.createDir(source, "/tmp/test", null);
        assert client.createDir(source, "/tmp/test", (short) 7);
    }

    /**
     * 判断文件是否存在
     */
    @Test
    public void isFileExist() throws Exception{
        assert client.isFileExist(source, "/tmp/test.txt");
    }

    /**
     * 文件检测并删除
     * @throws Exception
     */
    @Test
    public void checkAndDelete() throws Exception {
        assert client.checkAndDelete(source, "/tmp/test111.txt");
    }

    /**
     * 获取文件夹大小
     * @throws Exception
     */
    @Test
    public void getDirSize() throws Exception {
        client.getDirSize(source, "/tmp");
    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void deleteFiles () throws Exception {
        assert client.deleteFiles(source, Lists.newArrayList("/tmp/test1.txt"));
    }

    /**
     * 判断文件夹是否存在
     */
    @Test
    public void isDirExist() throws Exception{
        assert client.isDirExist(source, "/tmp");
    }

    /**
     * 设置路径权限
     */
    @Test
    public void setPermission() throws Exception{
        assert client.setPermission(source, "/tmp/test.txt", "ugoa=rwx");
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        assert client.rename(source, "/tmp/test.txt", "/tmp/test1.txt");
        assert client.rename(source, "/tmp/test1.txt", "/tmp/test.txt");
    }

    /**
     * hdfs内文件复制 - 覆盖
     * @throws Exception
     */
    @Test
    public void copyFileOverwrite() throws Exception{
        assert client.copyFile(source, "/tmp/test.txt", "/tmp/test.txt", true);
    }

    /**
     * 获取目标路径下所有文件名
     */
    @Test
    public void listAllFilePath() throws Exception {
        List<String> result = client.listAllFilePath(source, "/tmp/history");
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 递归获取
     */
    @Test
    public void listAllFiles() throws Exception {
        List<FileStatus> result = client.listAllFiles(source, "/tmp/history", true);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 非递归获取
     */
    @Test
    public void listAllFilesNoIterate() throws Exception {
        List<FileStatus> result = client.listAllFiles(source, "/tmp/history", false);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 从hdfs上copy文件到本地
     */
    @Test
    public void copyToLocal() throws Exception {
        assert client.copyToLocal(source, "/tmp/test.txt", localKerberosPath);
    }

    /**
     * copy本地文件到hdfs - 覆盖
     * @throws Exception
     */
    @Test
    public void copyFromLocal() throws Exception {
        assert client.copyFromLocal(source, localKerberosPath + "/test.txt", "/tmp/test.txt", true);
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByName() throws Exception {
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByName () throws Exception {
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByName () throws Exception {
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 下载 text格式的文件
     * @throws Exception
     */
    @Test
    public void getTextDownloader() throws Exception{
        client.uploadLocalFileToHdfs(source, localKerberosPath + "/textfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/textfile", Lists.newArrayList("id", "name"),",", FileFormat.TEXT.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.checkAndDelete(source, "/tmp/textfile");
    }

    /**
     * 下载parquet格式的文件
     * @throws Exception
     */
    @Test
    public void getParquetDownloader() throws Exception {
        client.uploadLocalFileToHdfs(source, localKerberosPath + "/parquetfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/parquetfile", Lists.newArrayList("id", "name"),",", FileFormat.PARQUET.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.checkAndDelete(source, "/tmp/parquetfile");
    }

    /**
     * 下载orc格式的文件
     * @throws Exception
     */
    @Test
    public void getOrcDownloader() throws Exception {
        client.uploadLocalFileToHdfs(source, localKerberosPath + "/orcfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/orcfile", Lists.newArrayList("id", "name"), ",", FileFormat.ORC.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.checkAndDelete(source, "/tmp/orcfile");
    }

    /**
     * 获取hdfs上存储的文件的字段信息 - 暂时只支持orc格式
     */
    @Test
    public void getColumnList () throws Exception {
        client.uploadLocalFileToHdfs(source, localKerberosPath + "/orcfile", "/tmp/");
        List<ColumnMetaDTO> columnList = client.getColumnList(source, SqlQueryDTO.builder().tableName("/tmp/orcfile").build(), FileFormat.ORC.getVal());
        assert CollectionUtils.isNotEmpty(columnList);
        client.checkAndDelete(source, "/tmp/orcfile");
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByPos() throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByPos () throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByPos () throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }
}
