package com.dtstack.dtcenter.common.loader.hdfs.client;

import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsFileDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsORCDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsParquetDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.HdfsTextDownload;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.YarnDownload;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsOrcWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsParquetWriter;
import com.dtstack.dtcenter.common.loader.hdfs.hdfswriter.HdfsTextWriter;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;
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

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:50 2020/8/10
 * @Description：HDFS 文件操作实现类
 */
public class HdfsFileClient implements IHdfsFile {

    private static final String PATH_DELIMITER = "/";

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
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getUser(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType(), hdfsSourceDTO.getKerberosConfig());
                        yarnDownload.configure();
                        return yarnDownload;
                    } catch (Exception e) {
                        throw new DtLoaderException("创建下载器异常", e);
                    }
                }
        );
    }

    @Override
    public IDownloader getFileDownloader(ISourceDTO iSource, String path) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        HdfsFileDownload hdfsFileDownload = new HdfsFileDownload(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), path, hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getKerberosConfig());
                        hdfsFileDownload.configure();
                        return hdfsFileDownload;
                    } catch (Exception e) {
                        throw new DtLoaderException("创建文件下载器异常", e);
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

        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.getFileStatus(fs, location);
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        HdfsOperator.copyToLocal(fs, remotePath, localDir);
        return true;
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        HdfsOperator.uploadLocalFileToHdfs(fs, localFilePath, remotePath);
        return true;
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.uploadInputStreamToHdfs(fs, bytes, remotePath);
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.createDir(fs, remotePath, permission);
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.isFileExist(fs, remotePath);
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.checkAndDelete(fs, remotePath);
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.getDirSize(fs, remotePath);
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.deleteFiles(fs, fileNames);
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.isDirExist(fs, remotePath);
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.setPermission(fs, remotePath, mode);
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.rename(fs, src, dist);
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.copyFile(fs, src, dist, isOverwrite);
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.listAllFilePath(fs, remotePath);
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return listFiles(fs, remotePath, isIterate);
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.copyToLocal(fs, srcPath, dstPath);
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        FileSystem fs = HdfsOperator.getFileSystem(hdfsSourceDTO.getKerberosConfig(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getDefaultFS());
        return HdfsOperator.copyFromLocal(fs, srcPath, dstPath, overwrite);
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        return createDownloader(hdfsSourceDTO, tableLocation, columnNames, fieldDelimiter, fileFormat, hdfsSourceDTO.getKerberosConfig());
                    } catch (Exception e) {
                        throw new DtLoaderException("创建下载器异常", e);
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

        throw new DtLoaderException("暂不支持该存储类型文件写入hdfs");
    }

    @Override
    public List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws
            Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return getColumnListOnFileFormat(hdfsSourceDTO, queryDTO, fileFormat);
    }

    @Override
    public int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByPosWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtLoaderException("获取hdfs文件字段信息异常", e);
                    }
                }
        );
    }

    @Override
    public int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        return KerberosLoginUtil.loginWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Integer>) () -> {
                    try {
                        return writeByNameWithFileFormat(hdfsSourceDTO, hdfsWriterDTO);
                    } catch (Exception e) {
                        throw new DtLoaderException("获取hdfs文件字段信息异常", e);
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
        throw new DtLoaderException("暂不支持该存储类型文件写入hdfs");
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
        throw new DtLoaderException("暂不支持该存储类型文件写入hdfs");
    }

    private List<ColumnMetaDTO> getColumnListOnFileFormat(HdfsSourceDTO hdfsSourceDTO, SqlQueryDTO queryDTO, String
            fileFormat) throws IOException {

        if (FileFormat.ORC.getVal().equals(fileFormat)) {
            return getOrcColumnList(hdfsSourceDTO, queryDTO);
        }

        throw new DtLoaderException("暂时不支持该存储类型的文件字段信息获取");
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

        if (file.matches(".*\\$\\{.*\\}.*")) {
            return path.substring(0, pos);
        }

        return path;
    }

    private List<FileStatus> listFiles(FileSystem fs, String remotePath, boolean isIterate) throws IOException {
        List<FileStatus> fileStatusList = new ArrayList<>();
        List<org.apache.hadoop.fs.FileStatus> fileStatuses = HdfsOperator.listFiles(fs, remotePath, isIterate);
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
