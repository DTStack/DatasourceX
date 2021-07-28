package com.dtstack.dtcenter.common.loader.hdfs.client;

import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HadoopConfUtil;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hdfs.YarnConfUtil;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsFileDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsORCDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsParquetDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsTextDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.YarnLogDownload.YarnTFileDownload;
import com.dtstack.dtcenter.common.loader.hdfs.fileMerge.core.CombineMergeBuilder;
import com.dtstack.dtcenter.common.loader.hdfs.fileMerge.core.CombineServer;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsOrcWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsParquetWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsTextWriter;
import com.dtstack.dtcenter.common.loader.hdfs.util.StringUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSContentSummary;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:50 2020/8/10
 * @Description：HDFS 文件操作实现类
 */
@Slf4j
public class HdfsFileClient implements IHdfsFile {

    private static final String PATH_DELIMITER = "/";

    // yarn聚合日志格式，默认TFIle
    private static final String LOG_FORMAT = "yarn.log-aggregation.file-formats";

    @Override
    public FileStatus getStatus(ISourceDTO iSource, String location) {
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
    public IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createYarnLogDownload(hdfsSourceDTO);
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("create downloader exception,%s", e.getMessage()), e);
                    }
                }
        );
    }

    /**
     * 创建yarn 聚合日志下载器，区分ifile、tfile格式
     * @param hdfsSourceDTO 数据源信息
     * @return yarn日志下载器
     * @throws Exception 异常信息
     */
    private IDownloader createYarnLogDownload(HdfsSourceDTO hdfsSourceDTO) throws Exception {
        IDownloader yarnDownload;
        Configuration configuration = YarnConfUtil.getFullConfiguration(null, hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getKerberosConfig());
        String fileFormat = configuration.get(LOG_FORMAT);
        boolean containerFiledExists = Arrays.stream(HdfsSourceDTO.class.getDeclaredFields())
                .anyMatch(field -> "ContainerId".equalsIgnoreCase(field.getName()));
        if (StringUtils.isNotBlank(fileFormat) && StringUtils.containsIgnoreCase(fileFormat, "IFile")) {
            if (!containerFiledExists || StringUtils.isEmpty(hdfsSourceDTO.getContainerId())) {
                yarnDownload = new LogAggregationIndexedFileController(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getUser(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig(), null);
            } else {
                yarnDownload = new LogAggregationIndexedFileController(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getUser(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getContainerId());
            }
        } else {
            if (!containerFiledExists || StringUtils.isEmpty(hdfsSourceDTO.getContainerId())) {
                yarnDownload = new YarnTFileDownload(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getUser(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig());
            } else {
                yarnDownload = new YarnTFileDownload(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getUser(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getContainerId());
            }
        }
        yarnDownload.configure();
        return yarnDownload;
    }

    @Override
    public IDownloader getFileDownloader(ISourceDTO iSource, String path) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        HdfsFileDownload hdfsFileDownload = new HdfsFileDownload(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), path, hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getKerberosConfig());
                        hdfsFileDownload.configure();
                        return hdfsFileDownload;
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Create file downloader exception,%s", e.getMessage()), e);
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
    private org.apache.hadoop.fs.FileStatus getHadoopStatus(ISourceDTO source, String location) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;

        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.getFileStatus(fs, location);
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        HdfsOperator.copyToLocal(fs, remotePath, localDir);
        return true;
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        HdfsOperator.uploadLocalFileToHdfs(fs, localFilePath, remotePath);
        return true;
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.uploadInputStreamToHdfs(fs, bytes, remotePath);
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.createDir(fs, remotePath, permission);
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.isFileExist(fs, remotePath);
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.checkAndDelete(fs, remotePath);
    }

    @Override
    public boolean delete(ISourceDTO source, String remotePath, boolean recursive) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        Configuration conf = HadoopConfUtil.getHdfsConf(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
                        FileSystem fs = FileSystem.get(conf);
                        return fs.delete(new Path(remotePath), recursive);
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Target path deletion exception,%s", e.getMessage()), e);
                    }
                }
        );
    }

    @Override
    public boolean copyDirector(ISourceDTO source, String src, String dist) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        Path srcPath = new Path(src);
                        Path distPath = new Path(dist);
                        Configuration conf = HadoopConfUtil.getHdfsConf(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
                        FileSystem fs = FileSystem.get(conf);
                        if (fs.exists(srcPath)) {
                            //判断是不是文件夹
                            if (fs.isDirectory(srcPath)) {
                                if (!FileUtil.copy(fs, srcPath, fs, distPath, false, conf)) {
                                    throw new DtLoaderException("copy " + src + " to " + dist + " failed");
                                }
                            } else {
                                throw new DtLoaderException(src + "is not a directory");
                            }
                        } else {
                            throw new DtLoaderException(src + " is not exists");
                        }
                        return true;
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Target path deletion exception,%s", e.getMessage()), e);
                    }
                }
        );
    }

    @Override
    public boolean fileMerge(ISourceDTO source, String src, String mergePath, FileFormat fileFormat, Long maxCombinedFileSize, Long needCombineFileSizeLimit) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        Configuration conf = HadoopConfUtil.getHdfsConf(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
                        CombineServer build = new CombineMergeBuilder()
                                .sourcePath(src)
                                .mergedPath(mergePath)
                                .fileType(fileFormat)
                                .maxCombinedFileSize(maxCombinedFileSize)
                                .needCombineFileSizeLimit(needCombineFileSizeLimit)
                                .configuration(conf)
                                .build();
                        build.combine();
                        return true;
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("File merge exception：%s", e.getMessage()), e);
                    }
                }
        );
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.getDirSize(fs, remotePath);
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.deleteFiles(fs, fileNames);
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.isDirExist(fs, remotePath);
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.setPermission(fs, remotePath, mode);
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.rename(fs, src, dist);
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        try {
            return HdfsOperator.copyFile(fs, src, dist, isOverwrite);
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Copying files in hdfs is abnormal : %s", e.getMessage()), e);
        }
    }

    @Override
    public List<FileStatus> listStatus(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        try {
            return transferFileStatus(HdfsOperator.listStatus(fs, remotePath));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("The status of the file or folder under the target path is abnormal : %s", e.getMessage()), e);
        }
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        try {
            return HdfsOperator.listAllFilePath(fs, remotePath);
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Obtaining all files in the target path is abnormal : %s", e.getMessage()), e);
        }
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return listFiles(fs, remotePath, isIterate);
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.copyToLocal(fs, srcPath, dstPath);
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.copyFromLocal(fs, srcPath, dstPath, overwrite);
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(hdfsSourceDTO, tableLocation, columnNames, fieldDelimiter, fileFormat, hdfsSourceDTO.getKerberosConfig());
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("create downloader exception : %s", e.getMessage()), e);
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
    private IDownloader createDownloader(HdfsSourceDTO hdfsSourceDTO, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat, Map<String, Object> kerberosConfig) throws Exception {
        if (FileFormat.TEXT.getVal().equals(fileFormat)) {
            HdfsTextDownload hdfsTextDownload = new HdfsTextDownload(hdfsSourceDTO, tableLocation, columnNames, fieldDelimiter, null, kerberosConfig);
            hdfsTextDownload.configure();
            return hdfsTextDownload;
        }

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            HdfsORCDownload hdfsORCDownload = new HdfsORCDownload(hdfsSourceDTO, tableLocation, columnNames, null, kerberosConfig);
            hdfsORCDownload.configure();
            return hdfsORCDownload;
        }

        if (FileFormat.PARQUET.getVal().equals(fileFormat)) {
            HdfsParquetDownload hdfsParquetDownload = new HdfsParquetDownload(hdfsSourceDTO, tableLocation, columnNames, null, kerberosConfig);
            hdfsParquetDownload.configure();
            return hdfsParquetDownload;
        }

        throw new DtLoaderException("This storage type file is not currently supported for writing to hdfs");
    }

    @Override
    public List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        try {
            return getColumnListOnFileFormat(hdfsSourceDTO, queryDTO, fileFormat);
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Failed to get column information : %s", e.getMessage()), e);
        }
    }

    @Override
    public int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByPosWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Obtaining the field information of the hdfs file is abnormal : %s", e.getMessage()), e);
                    }
                }
        );
    }

    @Override
    public int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByNameWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Obtaining the field information of the hdfs file is abnormal : %s", e.getMessage()), e);
                    }
                }
        );
    }

    @Override
    public HDFSContentSummary getContentSummary(ISourceDTO source, String hdfsDirPath) {
        return getContentSummary(source, Lists.newArrayList(hdfsDirPath)).get(0);
    }

    @Override
    public List<HDFSContentSummary> getContentSummary(ISourceDTO source, List<String> hdfsDirPaths){
        if (CollectionUtils.isEmpty(hdfsDirPaths)) {
            throw new DtLoaderException("hdfs path cannot be empty！");
        }
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        List<HDFSContentSummary> hdfsContentSummaries = Lists.newArrayList();
        // kerberos认证
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<HDFSContentSummary>>) () -> {
                    try {
                        Configuration conf = HadoopConfUtil.getHdfsConf(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
                        FileSystem fs = FileSystem.get(conf);
                        for (String hdfsDirPath : hdfsDirPaths) {
                            Path hdfsPath = new Path(hdfsDirPath);
                            if (!fs.exists(hdfsPath)) {
                                log.warn("hdfs path : {} does not exist", hdfsDirPath);
                                HDFSContentSummary contentSummaryEmpty = HDFSContentSummary.builder()
                                        .directoryCount(0L)
                                        .fileCount(0L)
                                        .ModifyTime(0L)
                                        .spaceConsumed(0L).build();
                                hdfsContentSummaries.add(contentSummaryEmpty);
                                continue;
                            }
                            org.apache.hadoop.fs.FileStatus fileStatus = fs.getFileStatus(hdfsPath);
                            ContentSummary contentSummary = fs.getContentSummary(hdfsPath);
                            HDFSContentSummary hdfsContentSummary = HDFSContentSummary.builder()
                                    .directoryCount(contentSummary.getDirectoryCount())
                                    .fileCount(contentSummary.getFileCount())
                                    .ModifyTime(fileStatus.getModificationTime())
                                    .spaceConsumed(contentSummary.getLength()).build();
                            hdfsContentSummaries.add(hdfsContentSummary);
                        }
                        return hdfsContentSummaries;
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Failed to obtain HDFS file information：%s", e.getMessage()), e);
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
        throw new DtLoaderException("This storage type file is not supported for writing to hdfs");
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
        throw new DtLoaderException("This storage type file is not supported for writing to hdfs");
    }

    private List<ColumnMetaDTO> getColumnListOnFileFormat(HdfsSourceDTO hdfsSourceDTO, SqlQueryDTO queryDTO, String
            fileFormat) throws IOException {

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            return getOrcColumnList(hdfsSourceDTO, queryDTO);
        }

        throw new DtLoaderException("The file field information acquisition of this storage type is not supported");
    }

    private List<ColumnMetaDTO> getOrcColumnList(HdfsSourceDTO hdfsSourceDTO, SqlQueryDTO queryDTO) throws IOException {
        ArrayList<ColumnMetaDTO> columnList = new ArrayList<>();
        Configuration conf = HdfsOperator.getConfig(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
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
                if (fileStatus.isFile() && fileStatus.getLen() > 49) {
                    Path subPath = fileStatus.getPath();
                    reader = OrcFile.createReader(subPath, readerOptions);
                    typeStruct = reader.getObjectInspector().getTypeName();
                    if (StringUtils.isNotEmpty(typeStruct)) {
                        break;
                    }
                }
            }
            if (reader == null) {
                throw new DtLoaderException("orcfile dir is empty!");
            }

        } else {
            reader = OrcFile.createReader(path, readerOptions);
            typeStruct = reader.getObjectInspector().getTypeName();
        }

        if (StringUtils.isEmpty(typeStruct)) {
            throw new DtLoaderException("can't retrieve type struct from " + path);
        }

        int startIndex = typeStruct.indexOf("<") + 1;
        int endIndex = typeStruct.lastIndexOf(">");
        typeStruct = typeStruct.substring(startIndex, endIndex);
        List<String> cols = StringUtil.splitIgnoreQuota(typeStruct, ',');
        for (String col : cols) {
            List<String> colNameAndType = StringUtil.splitIgnoreQuota(col, ':');
            if (CollectionUtils.isEmpty(colNameAndType) || colNameAndType.size() != 2) {
                continue;
            }
            ColumnMetaDTO metaDTO = new ColumnMetaDTO();
            metaDTO.setKey(colNameAndType.get(0));
            metaDTO.setType(colNameAndType.get(1));
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

        if (file.matches(".*\\$\\{.*\\}.*")) {
            return path.substring(0, pos);
        }

        return path;
    }

    private List<FileStatus> listFiles(FileSystem fs, String remotePath, boolean isIterate) {
        try {
            return transferFileStatus(HdfsOperator.listFiles(fs, remotePath, isIterate));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Failed to get the file in the target path : %s", e.getMessage()), e);
        }
    }

    /**
     * Apache Status 转换
     *
     * @param fileStatuses
     * @return
     */
    private List<FileStatus> transferFileStatus(List<org.apache.hadoop.fs.FileStatus> fileStatuses) {
        List<FileStatus> fileStatusList = new ArrayList<>();
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
