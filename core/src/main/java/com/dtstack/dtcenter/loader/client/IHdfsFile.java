package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:33 2020/8/10
 * @Description：HDFS 文件操作
 */
public interface IHdfsFile {
    /**
     * 获取 HDFS 对应地址文件信息
     *
     * @param source
     * @param location
     * @return
     * @throws Exception
     */
    FileStatus getStatus(ISourceDTO source, String location);

    /**
     * 日志下载器
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO);

    /**
     * 文件下载器
     *
     * @param iSource
     * @param path
     * @return
     * @throws Exception
     */
    IDownloader getFileDownloader(ISourceDTO iSource, String path);

    /**
     * 从 HDFS 上下载文件或文件夹到本地
     *
     * @param source
     * @param remotePath
     * @param localDir
     * @return
     * @throws Exception
     */
    boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir);

    /**
     * 上传文件到 HDFS
     *
     * @param source
     * @param localFilePath
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath);

    /**
     * 上传字节流到 HDFS
     *
     * @param source
     * @param bytes
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath);

    /**
     * 创建 HDFS 路径
     *
     * @param source
     * @param remotePath
     * @param permission
     * @return
     * @throws Exception
     */
    boolean createDir(ISourceDTO source, String remotePath, Short permission);

    /**
     * 路径文件是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isFileExist(ISourceDTO source, String remotePath);

    /**
     * 文件检测并删除
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean checkAndDelete(ISourceDTO source, String remotePath);

    /**
     * 获取路径文件大小
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    long getDirSize(ISourceDTO source, String remotePath);

    /**
     * 删除文件
     *
     * @param source
     * @param fileNames
     * @return
     * @throws Exception
     */
    boolean deleteFiles(ISourceDTO source, List<String> fileNames);

    /**
     * 路径目录是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isDirExist(ISourceDTO source, String remotePath);

    /**
     * 设置路径权限
     *
     * @param source
     * @param remotePath
     * @param mode
     * @return
     * @throws Exception
     */
    boolean setPermission(ISourceDTO source, String remotePath, String mode);

    /**
     * 重命名
     *
     * @param source
     * @param src
     * @param dist
     * @return
     * @throws Exception
     */
    boolean rename(ISourceDTO source, String src, String dist);

    /**
     * 复制文件
     *
     * @param source
     * @param src
     * @param dist
     * @param isOverwrite
     * @return
     * @throws Exception
     */
    boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite);

    /**
     * 获取目录下所有文件
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    List<String> listAllFilePath(ISourceDTO source, String remotePath);

    /**
     * 获取目录下所有文件的属性集
     *
     * @param source
     * @param remotePath
     * @param isIterate  是否递归
     * @return
     * @throws Exception
     */
    List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate);

    /**
     * 从hdfs copy文件到本地
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @return
     * @throws Exception
     */
    boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath);

    /**
     * 从本地copy到hdfs
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @param overwrite 是否重写
     * @return
     * @throws Exception
     */
    boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite);

    /**
     * 根据文件格式获取对应的downlaoder
     *
     * @param source
     * @param tableLocation
     * @param fieldDelimiter
     * @param fileFormat
     * @return
     * @throws Exception
     */
    IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat);

    /**
     * 获取hdfs上存储文件的字段信息
     *
     * @param source
     * @param queryDTO
     * @param fileFormat
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat);

    /**
     * 按位置写入
     *
     * @param source
     * @param hdfsWriterDTO
     * @return
     * @throws Exception
     */
    int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO);

    /**
     * 从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
     * ---需要根据column信息判断导入的数据是否符合要求
     *
     * @param source
     * @param hdfsWriterDTO
     * @return
     * @throws Exception
     */
    int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO);

}
