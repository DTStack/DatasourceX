package com.dtstack.dtcenter.common.loader.hdfs.client;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hdfs.HdfsConnFactory;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsORCDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsParquetDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsTextDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.YarnDownload;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsOrcWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsParquetWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsTextWriter;
import com.dtstack.dtcenter.common.loader.hdfs.util.KerberosUtil;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.IHdfsWriter;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.sql.Twins;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:50 2020/8/10
 * @Description：HDFS 文件操作实现类
 */
public class HdfsFileClient implements IHdfsFile {

    private static final String PATH_DELIMITER = "/";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public FileStatus getStatus(ISourceDTO iSource, String location) throws Exception {
        org.apache.hadoop.fs.FileStatus hadoopFileStatus = getHadoopStatus(iSource, location);

        return FileStatus.builder()
                .length(hadoopFileStatus.getLen())
                .access_time(hadoopFileStatus.getAccessTime())
                .block_replication(hadoopFileStatus.getReplication())
                .blocksize(hadoopFileStatus.getBlockSize())
                .group(hadoopFileStatus.getGroup())
                .isdir(hadoopFileStatus.isDirectory())
                .modification_time(hadoopFileStatus.getModificationTime())
                .owner(hadoopFileStatus.getOwner())
                .path(hadoopFileStatus.getPath().toString())
                .build();
    }

    @Override
    public IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType());
            yarnDownload.configure();
            return yarnDownload;
        }

        // 校验高可用配置
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType());
                        yarnDownload.configure();
                        return yarnDownload;
                    } catch (Exception e) {
                        throw new DtCenterDefException("创建下载器异常", e);
                    }
                }
        );
    }

    /**
     * 获取 HADOOP 文件信息
     *
     * @param iSource
     * @param location
     * @return
     * @throws Exception
     */
    private org.apache.hadoop.fs.FileStatus getHadoopStatus(ISourceDTO iSource, String location) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        if (!hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }

        Properties properties = HdfsConnFactory.combineHdfsConfig(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
        Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), properties);
        //不在做重复认证 主要用于 HdfsOperator.checkConnection 中有一些数栈自己的逻辑
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");

        org.apache.hadoop.fs.FileStatus hadoopFileStatus = null;
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            hadoopFileStatus = HdfsOperator.getFileStatus(conf, location);
        }

        hadoopFileStatus = KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<org.apache.hadoop.fs.FileStatus>) () -> {
                    try {
                        return HdfsOperator.getFileStatus(conf, location);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs 文件状态异常", e);
                    }
                }
        );

        return hadoopFileStatus;
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception {
        return false;
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception {
        return false;
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) throws Exception {
        return 0;
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception {
        return false;
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception {
        return false;
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) throws Exception {
        return false;
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception {
        return false;
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception {
        return null;
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception {
        return null;
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception {
        return false;
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception {
        return false;
    }

    @Override
    public boolean copyInHDFS(ISourceDTO source1, ISourceDTO source2, String srcPath, String dstPath, String localPath, boolean overwrite) throws Exception {
        return false;
    }

    @Override
    public IHdfsWriter getHdfsWriter(String fileFormat) throws Exception {
        IHdfsWriter hdfsWriter = null;
        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            hdfsWriter =  new HdfsOrcWriter();
        }else if (FileFormat.PARQUET.getVal().equals(fileFormat)) {
            hdfsWriter =  new HdfsParquetWriter();
        }else if (FileFormat.TEXT.getVal().equals(fileFormat)) {
            hdfsWriter =  new HdfsTextWriter();
        }
        if (hdfsWriter != null) {
            return hdfsWriter;
        }else {
            throw new DtCenterDefException("暂不支持该存储类型文件写入hdfs");
        }

    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, String fieldDelimiter, String fileFormat) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            try {
                return createDownloader(hdfsSourceDTO, tableLocation, fieldDelimiter, fileFormat);
            }catch (Exception e) {
                throw new DtCenterDefException("创建下载器异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(hdfsSourceDTO, tableLocation, fieldDelimiter, fileFormat);
                    } catch (Exception e) {
                        throw new DtCenterDefException("创建下载器异常", e);
                    }
                }
        );

    }

    /**
     * 根据存储格式创建对应的hdfs下载器
     *
     * @param hdfsSourceDTO
     * @param tableLocation
     * @param fieldDelimiter
     * @param fileFormat
     * @return
     */
    private IDownloader createDownloader(HdfsSourceDTO hdfsSourceDTO, String tableLocation, String fieldDelimiter, String fileFormat) throws Exception {
        if (FileFormat.TEXT.getVal().equals(fileFormat)) {
            HdfsTextDownload hdfsTextDownload = new HdfsTextDownload(hdfsSourceDTO, tableLocation, null, fieldDelimiter, null);
            hdfsTextDownload.configure();
            return hdfsTextDownload;
        }

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            HdfsORCDownload hdfsORCDownload = new HdfsORCDownload(hdfsSourceDTO, tableLocation, null, null);
            hdfsORCDownload.configure();
            return hdfsORCDownload;
        }

        if (FileFormat.PARQUET.getVal().equals(fileFormat)) {
            HdfsParquetDownload hdfsParquetDownload = new HdfsParquetDownload(hdfsSourceDTO, tableLocation, null, null);
            hdfsParquetDownload.configure();
            return hdfsParquetDownload;
        }

        throw new DtCenterDefException("暂不支持该存储类型文件写入hdfs");
    }

    @Override
    public List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            try {
                return getColumnListOnFileFormat(hdfsSourceDTO, queryDTO, fileFormat);
            }catch (Exception e) {
                throw new DtCenterDefException("创建下载器异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<ColumnMetaDTO>>) () -> {
                    try {
                        return getColumnListOnFileFormat(hdfsSourceDTO, queryDTO, fileFormat);
                    } catch (Exception e) {
                        throw new DtCenterDefException("创建下载器异常", e);
                    }
                }
        );

    }

    private List<ColumnMetaDTO> getColumnListOnFileFormat(HdfsSourceDTO hdfsSourceDTO, SqlQueryDTO queryDTO, String fileFormat) throws IOException {

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            return getOrcColumnList(hdfsSourceDTO, queryDTO);
        }

        throw new DtCenterDefException("暂时不支持该存储类型的文件字段信息获取");
    }

    private List<ColumnMetaDTO> getOrcColumnList(HdfsSourceDTO hdfsSourceDTO, SqlQueryDTO queryDTO) throws IOException {
        ArrayList<ColumnMetaDTO> columnList = new ArrayList<>();
        Properties props = objectMapper.readValue(hdfsSourceDTO.getConfig(), Properties.class);
        Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), props);
        FileSystem fs = HdfsOperator.getFileSystem(conf);
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
        readerOptions.filesystem(fs);
        String fileName = hdfsSourceDTO.getDefaultFS() + PATH_DELIMITER + queryDTO.getTableName();
        fileName = handleVariable(fileName);

        Path path = new Path(fileName);
        org.apache.hadoop.hive.ql.io.orc.Reader reader = null;
        String typeStruct = null;
        if (fs.isDirectory(path)) {
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);
            while (iterator.hasNext()) {
                org.apache.hadoop.fs.FileStatus fileStatus = iterator.next();
                if(fileStatus.isFile() && fileStatus.getLen() > 49) {
                    Path subPath = fileStatus.getPath();
                    reader = OrcFile.createReader(subPath, readerOptions);
                    typeStruct = reader.getObjectInspector().getTypeName();
                    if(StringUtils.isNotEmpty(typeStruct)) {
                        break;
                    }
                }
            }
            if(reader == null) {
                throw new DtCenterDefException("orcfile dir is empty!");
            }

        } else {
            reader = OrcFile.createReader(path, readerOptions);
            typeStruct = reader.getObjectInspector().getTypeName();
        }

        if (StringUtils.isEmpty(typeStruct)) {
            throw new DtCenterDefException("can't retrieve type struct from " + path);
        }

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);
        String[] cols = typeStruct.split(",");

        for (int i = 0; i < cols.length; ++i) {
            String[] temp = cols[i].split(":");
            ColumnMetaDTO metaDTO = new ColumnMetaDTO();
            metaDTO.setKey(temp[0]);
            metaDTO.setType(temp[1]);
            columnList.add(metaDTO);
        }

        return columnList;
    }

    private static String handleVariable(String path) {
        if (path.endsWith(PATH_DELIMITER)) {
            path = path.substring(0, path.length() - 1);
        }

        int pos = path.lastIndexOf(PATH_DELIMITER);
        String file = path.substring(pos + 1, path.length());

        if(file.matches(".*\\$\\{.*\\}.*")) {
            return path.substring(0, pos);
        }

        return path;
    }

}
