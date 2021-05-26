package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSImportColumn;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:44 2020/9/8
 * @Description：hdfs 文件 Kerberos 测试
 */
@Ignore
public class HdfsFileKerberosTest extends BaseTest {

    // 初始化客户端
    private static final IHdfsFile client = ClientCache.getHdfs(DataSourceType.HDFS.getVal());

    private static HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://eng-cdh1:8020")
            .build();

    private static Hive1SourceDTO hiveSource = Hive1SourceDTO.builder()
            .url("jdbc:hive2://eng-cdh3:10001/default;principal=hive/eng-cdh3@DTSTACK.COM")
            .defaultFS("hdfs://eng-cdh1:8020")
            .build();

    private static HdfsWriterDTO writerDTO;

    private static final String localFilePath = HdfsFileTest.class.getResource("/hdfs-file").getPath();

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("HADOOP_USER_NAME", "admin");
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hive-cdh03.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        String localKerberosPath = HdfsFileKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HDFS.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
        hiveSource.setKerberosConfig(kerberosConfig);
        source.setKerberosConfig(kerberosConfig);
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).delete(source, "/tmp/hive_test", true);

        // 创建parquet格式的hive外部表，并插入数据
        IClient hiveClient = ClientCache.getClient(DataSourceType.HIVE1X.getVal());
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
        writerDTO.setFromFileName(localFilePath + "/textfile");
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
        client.copyFromLocal(source, localFilePath + "/test.txt", "/tmp/loader_test.txt", true);
    }

    /**
     * 测试hdfs连通性
     * @
     */
    @Test
    public void testConn() {
        IClient client = ClientCache.getClient(DataSourceType.HDFS.getVal());
        assert  client.testCon(source);
    }

    /**
     * 获取文件状态
     * @
     */
    @Test
    public void getStatus ()  {
        FileStatus fileStatus = client.getStatus(source, "/tmp");
        Assert.assertNotNull(fileStatus);
    }

    /**
     * 下载hdfs文件到本地
     */
    @Test
    public void downloadFileFromHdfs()  {
        client.downloadFileFromHdfs(source, "/tmp/loader_test.txt", localFilePath);
    }

    /**
     * 上传本地文件到hdfs
     */
    @Test
    public void uploadLocalFileToHdfs() {
        assert client.uploadLocalFileToHdfs(source, localFilePath + "/loader_test.txt", "/tmp");
    }

    /**
     * 上传字节流文件到hdfs
     */
    @Test
    public void uploadInputStreamToHdfs() {
        assert client.uploadInputStreamToHdfs(source, "test".getBytes(), "/tmp/loader_test.txt");
    }

    /**
     * 在hdfs文件系统创建文件夹
     */
    @Test
    public void createDir()  {
        client.delete(source, "/tmp/test", true);
        assert client.createDir(source, "/tmp/test", null);
        assert client.createDir(source, "/tmp/test", (short) 7);
    }

    /**
     * 判断文件是否存在
     */
    @Test
    public void isFileExist() {
        assert client.isFileExist(source, "/tmp/loader_test.txt");
    }

    /**
     * 文件检测并删除
     * @
     */
    @Test
    public void checkAndDelete()  {
        client.delete(source, "/tmp/test111.txt", true);
    }

    /**
     * 获取文件夹大小
     * @
     */
    @Test
    public void getDirSize()  {
        client.getDirSize(source, "/tmp");
    }

    /**
     * 删除文件
     * @
     */
    @Test
    public void deleteFiles ()  {
        assert client.deleteFiles(source, Lists.newArrayList("/tmp/test1.txt"));
    }

    /**
     * 判断文件夹是否存在
     */
    @Test
    public void isDirExist() {
        assert client.isDirExist(source, "/tmp");
    }

    /**
     * 设置路径权限
     */
    @Test
    public void setPermission() {
        assert client.setPermission(source, "/tmp/loader_test.txt", "ugoa=rwx");
    }

    /**
     * 重命名
     */
    @Test
    public void rename()  {
        assert client.rename(source, "/tmp/loader_test.txt", "/tmp/test1.txt");
        assert client.rename(source, "/tmp/test1.txt", "/tmp/loader_test.txt");
    }

    /**
     * hdfs内文件复制 - 覆盖
     * @
     */
    @Test
    public void copyFileOverwrite() {
        assert client.copyFile(source, "/tmp/loader_test.txt", "/tmp/loader_test.txt", true);
    }

    /**
     * 获取目标路径下所有文件名
     */
    @Test
    public void listAllFilePath()  {
        List<String> result = client.listAllFilePath(source, "/tmp/hive_test");
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 递归获取
     */
    @Test
    public void listAllFiles()  {
        List<FileStatus> result = client.listAllFiles(source, "/tmp/hive_test", true);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 非递归获取
     */
    @Test
    public void listAllFilesNoIterate()  {
        List<FileStatus> result = client.listAllFiles(source, "/tmp/hive_test", false);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 从hdfs上copy文件到本地
     */
    @Test
    public void copyToLocal()  {
        assert client.copyToLocal(source, "/tmp/loader_test.txt", localFilePath);
    }

    /**
     * copy本地文件到hdfs - 覆盖
     * @
     */
    @Test
    public void copyFromLocal()  {
        assert client.copyFromLocal(source, localFilePath + "/loader_test.txt", "/tmp/loader_test.txt", true);
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByName()  {
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByName ()  {
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByName ()  {
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByName(source, writerDTO) > 0;
    }

    /**
     * 下载 text格式的文件
     * @
     */
    @Test
    public void getTextDownloader() {
        client.uploadLocalFileToHdfs(source, localFilePath + "/textfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/textfile", Lists.newArrayList("id", "name"),",", FileFormat.TEXT.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.delete(source, "/tmp/textfile", true);
    }

    /**
     * 下载parquet格式的文件
     * @
     */
    @Test
    public void getParquetDownloader()  {
        client.uploadLocalFileToHdfs(source, localFilePath + "/parquetfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/parquetfile", Lists.newArrayList("id", "name"),",", FileFormat.PARQUET.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.delete(source, "/tmp/parquetfile", true);
    }

    /**
     * 下载orc格式的文件
     * @
     */
    @Test
    public void getOrcDownloader()  {
        client.uploadLocalFileToHdfs(source, localFilePath + "/orcfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = client.getDownloaderByFormat(source, "/tmp/orcfile", Lists.newArrayList("id", "name"), ",", FileFormat.ORC.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        client.delete(source, "/tmp/orcfile", true);
    }

    /**
     * 获取hdfs上存储的文件的字段信息 - 暂时只支持orc格式
     */
    @Test
    public void getColumnList ()  {
        client.uploadLocalFileToHdfs(source, localFilePath + "/orcfile", "/tmp/");
        List<ColumnMetaDTO> columnList = client.getColumnList(source, SqlQueryDTO.builder().tableName("/tmp/orcfile").build(), FileFormat.ORC.getVal());
        assert CollectionUtils.isNotEmpty(columnList);
        client.delete(source, "/tmp/orcfile", true);
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByPos()  {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByPos ()  {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByPos ()  {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localFilePath + "/textfile");
        assert client.writeByPos(source, writerDTO) > 0;
    }
}
