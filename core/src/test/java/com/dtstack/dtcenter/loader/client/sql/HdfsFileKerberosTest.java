package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSImportColumn;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
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
public class HdfsFileKerberosTest {
    private static HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://eng-cdh1:8020")
            .build();

    private static HiveSourceDTO hiveSource = HiveSourceDTO.builder()
            .url("jdbc:hive2://eng-cdh1:10000/default;principal=hive/eng-cdh1@DTSTACK.COM")
            .schema("default")
            .defaultFS("hdfs://eng-cdh1:8020")
            .build();

    private static HdfsWriterDTO writerDTO;

    private String localKerberosPath = HdfsFileTest.class.getResource("/hdfs-file").getPath();

    @BeforeClass
    public static void beforeClass() throws Exception {
        // 准备 hdfs Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "hdfs/eng-cdh1@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hadoop.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);
        String localKerberosPath = HbaseKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HDFS.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
        System.setProperty("HADOOP_USER_NAME", "root");
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/hive_test");

        // 准备 Hive Kerberos 参数
        Map<String, Object> hive_kerberosConfig = new HashMap<>();
        hive_kerberosConfig.put(HadoopConfTool.PRINCIPAL, "hive/eng-cdh1@DTSTACK.COM");
        hive_kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/hive-cdh01.keytab");
        hive_kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        hiveSource.setKerberosConfig(hive_kerberosConfig);
        String hive_localKerberosPath = HiveKerberosTest.class.getResource("/eng-cdh").getPath();
        IKerberos hive_kerberos = ClientCache.getKerberos(DataSourceType.HIVE.getVal());
        hive_kerberos.prepareKerberosForConnect(hive_kerberosConfig, hive_localKerberosPath);

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
        assert client.testCon(source);
    }

    /**
     * 获取文件状态
     * @throws Exception
     */
    @Test
    public void getStatus () throws Exception {
        FileStatus fileStatus = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getStatus(source, "/tmp");
        Assert.assertNotNull(fileStatus);
    }

    /**
     * 下载hdfs文件到本地
     */
    @Test
    public void downloadFileFromHdfs() throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).downloadFileFromHdfs(source, "/tmp/test.txt", localKerberosPath);
    }

    /**
     * 上传本地文件到hdfs
     */
    @Test
    public void uploadLocalFileToHdfs() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadLocalFileToHdfs(source, localKerberosPath + "/test.txt", "/tmp");
    }

    /**
     * 上传字节流文件到hdfs
     */
    @Test
    public void uploadInputStreamToHdfs() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadInputStreamToHdfs(source, "test".getBytes(), "/tmp/test.txt");
    }

    /**
     * 在hdfs文件系统创建文件夹
     */
    @Test
    public void createDir() throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/test");
        short t = 14;
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).createDir(source, "/tmp/test", t);
    }

    /**
     * 判断文件是否存在
     */
    @Test
    public void isFileExist() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).isFileExist(source, "/tmp/test.txt");
    }

    /**
     * 文件检测并删除
     * @throws Exception
     */
    @Test
    public void checkAndDelete() throws Exception {
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/test111.txt");
    }

    /**
     * 获取文件夹大小
     * @throws Exception
     */
    @Test
    public void getDirSize() throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getDirSize(source, "/tmp");
    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void deleteFiles () throws Exception {
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).deleteFiles(source, Lists.newArrayList("/tmp/test1.txt"));
    }

    /**
     * 判断文件夹是否存在
     */
    @Test
    public void isDirExist() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).isDirExist(source, "/tmp");
    }

    /**
     * 设置路径权限
     */
    @Test
    public void setPermission() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).setPermission(source, "/tmp/test.txt", "ugoa=rwx");
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).rename(source, "/tmp/test.txt", "/tmp/test1.txt");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).rename(source, "/tmp/test1.txt", "/tmp/test.txt");
    }

    /**
     * hdfs内文件复制 - 覆盖
     * @throws Exception
     */
    @Test
    public void copyFileOverwrite() throws Exception{
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).copyFile(source, "/tmp/test.txt", "/tmp/test.txt", true);
    }

    /**
     * 获取目标路径下所有文件名
     */
    @Test
    public void listAllFilePath() throws Exception {
        List<String> result = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).listAllFilePath(source, "/tmp");
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 递归获取
     */
    @Test
    public void listAllFiles() throws Exception {
        List<FileStatus> result = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).listAllFiles(source, "/tmp", true);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 获取目标路径下所有文件属性集 - 非递归获取
     */
    @Test
    public void listAllFilesNoIterate() throws Exception {
        List<FileStatus> result = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).listAllFiles(source, "/tmp", false);
        assert CollectionUtils.isNotEmpty(result);
    }

    /**
     * 从hdfs上copy文件到本地
     */
    @Test
    public void copyToLocal() throws Exception {
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).copyToLocal(source, "/tmp/test.txt", localKerberosPath);
    }

    /**
     * copy本地文件到hdfs - 覆盖
     * @throws Exception
     */
    @Test
    public void copyFromLocal() throws Exception {
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).copyFromLocal(source, localKerberosPath + "/test.txt", "/tmp/test.txt", true);
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByName() throws Exception {
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByName () throws Exception {
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByName(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByName () throws Exception {
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByName(source, writerDTO) > 0;
    }

    /**
     * 下载 text格式的文件
     * @throws Exception
     */
    @Test
    public void getTextDownloader() throws Exception{
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadLocalFileToHdfs(source, localKerberosPath + "/textfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getDownloaderByFormat(source, "/tmp/textfile", Lists.newArrayList("id", "name"),",", FileFormat.TEXT.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/textfile");
    }

    /**
     * 下载parquet格式的文件
     * @throws Exception
     */
    @Test
    public void getParquetDownloader() throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadLocalFileToHdfs(source, localKerberosPath + "/parquetfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getDownloaderByFormat(source, "/tmp/parquetfile", Lists.newArrayList("id", "name"),",", FileFormat.PARQUET.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/parquetfile");
    }

    /**
     * 下载orc格式的文件
     * @throws Exception
     */
    @Test
    public void getOrcDownloader() throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadLocalFileToHdfs(source, localKerberosPath + "/orcfile", "/tmp/");
        source.setYarnConf(new HashMap<>());
        IDownloader downloader = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getDownloaderByFormat(source, "/tmp/orcfile", Lists.newArrayList("id", "name"), ",", FileFormat.ORC.getVal());
        System.out.println(downloader.getMetaInfo());
        while (!downloader.reachedEnd()) {
            System.out.println(downloader.readNext());
        }
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/orcfile");
    }

    /**
     * 获取hdfs上存储的文件的字段信息 - 暂时只支持orc格式
     */
    @Test
    public void getColumnList () throws Exception {
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).uploadLocalFileToHdfs(source, localKerberosPath + "/orcfile", "/tmp/");
        List<ColumnMetaDTO> columnList = ClientCache.getHdfs(DataSourceType.HDFS.getVal()).getColumnList(source, SqlQueryDTO.builder().tableName("/tmp/orcfile").build(), FileFormat.ORC.getVal());
        assert CollectionUtils.isNotEmpty(columnList);
        ClientCache.getHdfs(DataSourceType.HDFS.getVal()).checkAndDelete(source, "/tmp/orcfile");
    }

    /**
     * 写入text文件到hdfs
     */
    @Test
    public void writeTextByPos() throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.TEXT.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入parquet格式文件到hdfs
     */
    @Test
    public void writeParquetByPos () throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.PARQUET.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByPos(source, writerDTO) > 0;
    }

    /**
     * 写入orc格式文件到hdfs
     */
    @Test
    public void writeOrcByPos () throws Exception {
        writerDTO.setTopLineIsTitle(true);
        writerDTO.setFileFormat(FileFormat.ORC.getVal());
        writerDTO.setFromFileName(localKerberosPath + "/textfile");
        assert ClientCache.getHdfs(DataSourceType.HDFS.getVal()).writeByPos(source, writerDTO) > 0;
    }
}
