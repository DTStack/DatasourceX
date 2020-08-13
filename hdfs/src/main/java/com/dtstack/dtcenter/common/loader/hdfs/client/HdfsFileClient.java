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
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.downloader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
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
import java.util.Map;
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
            YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), null);
            yarnDownload.configure();
            return yarnDownload;
        }

        // 校验高可用配置
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig());
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
     * @param source
     * @param location
     * @return
     * @throws Exception
     */
    private org.apache.hadoop.fs.FileStatus getHadoopStatus(ISourceDTO source, String location) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        org.apache.hadoop.fs.FileStatus hadoopFileStatus = null;
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.getFileStatus(conf, location);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<org.apache.hadoop.fs.FileStatus>) () -> {
                    try {
                        return HdfsOperator.getFileStatus(conf, location);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs 文件状态异常", e);
                    }
                }
        );

    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.downloadFileFromHDFS(conf, remotePath, localDir);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.downloadFileFromHDFS(conf, remotePath, localDir);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("从hdfs下载文件异常", e);
                    }
                }
        );

    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.uploadLocalFileToHdfs(conf, localFilePath, remotePath);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.uploadLocalFileToHdfs(conf, localFilePath, remotePath);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("上传文件到hdfs异常", e);
                    }
                }
        );
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.uploadInputStreamToHdfs(conf, bytes, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.uploadInputStreamToHdfs(conf, bytes, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("上传文件到hdfs异常", e);
                    }
                }
        );
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.createDir(conf, remotePath, permission);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.createDir(conf, remotePath, permission);
                    } catch (Exception e) {
                        throw new DtCenterDefException("在hdfs创建文件夹异常", e);
                    }
                }
        );
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.isFileExist(conf, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.isFileExist(conf, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取文件是否存在异常", e);
                    }
                }
        );
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.checkAndDele(conf, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.checkAndDele(conf, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("文件检测异常", e);
                    }
                }
        );
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.getDirSize(conf, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Long>) () -> {
                    try {
                        return HdfsOperator.getDirSize(conf, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs 文件大小异常", e);
                    }
                }
        );
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.deleteFiles(conf, fileNames);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.deleteFiles(conf, fileNames);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("从 hdfs 删除文件异常", e);
                    }
                }
        );
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.isDirExist(conf, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.isDirExist(conf, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("判断文件夹是否存在异常", e);
                    }
                }
        );
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.setPermission(conf, remotePath, mode);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.setPermission(conf, remotePath, mode);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("hdfs权限设置异常", e);
                    }
                }
        );
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.rename(conf, src, dist);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return HdfsOperator.rename(conf, src, dist);
                    } catch (Exception e) {
                        throw new DtCenterDefException("hdfs 文件重命名异常", e);
                    }
                }
        );
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.copyFile(conf, src, dist, isOverwrite);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.copyFile(conf, src, dist, isOverwrite);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("hdfs内 文件复制异常", e);
                    }
                }
        );
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.listAllFilePath(conf, remotePath);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<String>>) () -> {
                    try {
                        return HdfsOperator.listAllFilePath(conf, remotePath);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs目录 文件异常", e);
                    }
                }
        );
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return listFiles(conf, remotePath, isIterate);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<FileStatus>>) () -> {
                    try {
                        return listFiles(conf, remotePath, isIterate);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs 目录下文件状态异常", e);
                    }
                }
        );
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.copyToLocal(conf, srcPath, dstPath);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.copyToLocal(conf, srcPath, dstPath);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("copy hdfs 文件到本地异常", e);
                    }
                }
        );
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        Configuration conf = getHadoopConf(hdfsSourceDTO);

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            HdfsOperator.copyFromLocal(conf, srcPath, dstPath, overwrite);
            return true;
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        HdfsOperator.copyFromLocal(conf, srcPath, dstPath, overwrite);
                        return true;
                    } catch (Exception e) {
                        throw new DtCenterDefException("从本地copy 文件到 hdfs异常", e);
                    }
                }
        );
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, String fieldDelimiter, String fileFormat) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            try {
                return createDownloader(hdfsSourceDTO, tableLocation, fieldDelimiter, fileFormat, null);
            }catch (Exception e) {
                throw new DtCenterDefException("创建下载器异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(hdfsSourceDTO, tableLocation, fieldDelimiter, fileFormat, hdfsSourceDTO.getKerberosConfig());
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
    private IDownloader createDownloader(HdfsSourceDTO hdfsSourceDTO, String tableLocation, String fieldDelimiter, String fileFormat, Map<String, Object> kerberosConfig) throws Exception {
        if (FileFormat.TEXT.getVal().equals(fileFormat)) {
            HdfsTextDownload hdfsTextDownload = new HdfsTextDownload(hdfsSourceDTO, tableLocation, null, fieldDelimiter, null, kerberosConfig);
            hdfsTextDownload.configure();
            return hdfsTextDownload;
        }

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            HdfsORCDownload hdfsORCDownload = new HdfsORCDownload(hdfsSourceDTO, tableLocation, null, null, kerberosConfig);
            hdfsORCDownload.configure();
            return hdfsORCDownload;
        }

        if (FileFormat.PARQUET.getVal().equals(fileFormat)) {
            HdfsParquetDownload hdfsParquetDownload = new HdfsParquetDownload(hdfsSourceDTO, tableLocation, null, null, kerberosConfig);
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
                throw new DtCenterDefException("获取hdfs文件字段信息异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<ColumnMetaDTO>>) () -> {
                    try {
                        return getColumnListOnFileFormat(hdfsSourceDTO, queryDTO, fileFormat);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取hdfs文件字段信息异常", e);
                    }
                }
        );

    }

    @Override
    public int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            try {
                return writeByPosWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
            }catch (Exception e) {
                throw new DtCenterDefException("写入hdfs异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByPosWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取hdfs文件字段信息异常", e);
                    }
                }
        );
    }

    @Override
    public int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            try {
                return writeByNameWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
            }catch (Exception e) {
                throw new DtCenterDefException("写入hdfs异常", e);
            }
        }

        // kerberos认证
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByNameWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取hdfs文件字段信息异常", e);
                    }
                }
        );
    }

    private int writeByPosWithFileFormat(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws IOException {
        if (FileFormat.ORC.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsOrcWriter.writeByPos(source, hdfsWriterDTO);
        }
        if (FileFormat.PARQUET.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsParquetWriter.writeByPos(source, hdfsWriterDTO);
        }
        if (FileFormat.TEXT.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsTextWriter.writeByPos(source, hdfsWriterDTO);
        }
        throw new DtCenterDefException("暂不支持该存储类型文件写入hdfs");
    }

    private int writeByNameWithFileFormat(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws IOException {
        if (FileFormat.ORC.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsOrcWriter.writeByName(source, hdfsWriterDTO);
        }
        if (FileFormat.PARQUET.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsParquetWriter.writeByName(source, hdfsWriterDTO);
        }
        if (FileFormat.TEXT.getVal().equals(hdfsWriterDTO.getFileFormat())) {
            return HdfsTextWriter.writeByName(source, hdfsWriterDTO);
        }
        throw new DtCenterDefException("暂不支持该存储类型文件写入hdfs");
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

    private Configuration getHadoopConf(HdfsSourceDTO hdfsSourceDTO){

        if (!hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }
        Properties properties = HdfsConnFactory.combineHdfsConfig(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
        Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), properties);
        //不在做重复认证 主要用于 HdfsOperator.checkConnection 中有一些数栈自己的逻辑
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");
        return conf;
    }

    private List<FileStatus> listFiles(Configuration conf, String remotePath, boolean isIterate) throws IOException {
        List<FileStatus> fileStatusList = new ArrayList<>();
        List<org.apache.hadoop.fs.FileStatus> fileStatuses = HdfsOperator.listFiles(conf, remotePath, isIterate);
        for (org.apache.hadoop.fs.FileStatus fileStatus : fileStatuses) {
            FileStatus fileStatusTemp = FileStatus.builder()
                    .length(fileStatus.getLen())
                    .access_time(fileStatus.getAccessTime())
                    .block_replication(fileStatus.getReplication())
                    .blocksize(fileStatus.getBlockSize())
                    .group(fileStatus.getGroup())
                    .isdir(fileStatus.isDirectory())
                    .modification_time(fileStatus.getModificationTime())
                    .owner(fileStatus.getOwner())
                    .path(fileStatus.getPath().toString())
                    .build();
            fileStatusList.add(fileStatusTemp);
        }
        return fileStatusList;
    }
}
