package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.IHdfsWriter;
import com.dtstack.dtcenter.loader.dto.FileStatus;
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
    FileStatus getStatus(ISourceDTO source, String location) throws Exception;

    /**
     * 日志下载器
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 从 HDFS 上下载文件或文件夹到本地
     *
     * @param source
     * @param remotePath
     * @param localDir
     * @throws Exception
     */
    boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception;

    /**
     * 上传问价到 HDFS
     *
     * @param source
     * @param localFilePath
     * @param remotePath
     * @throws Exception
     */
    boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception;

    /**
     * 上传字节流到 HDFS
     *
     * @param source
     * @param bytes
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception;

    /**
     * 创建 HDFS 路径
     *
     * @param source
     * @param remotePath
     * @param permission
     * @return
     * @throws Exception
     */
    boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception;

    /**
     * 路径文件是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isFileExist(ISourceDTO source, String remotePath) throws Exception;

    /**
     * 文件检测并删除
     *
     * @return
     * @throws Exception
     */
    boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception;

    /**
     * 获取路径文件大小
     *
     * @return
     * @throws Exception
     */
    long getDirSize(ISourceDTO source, String remotePath) throws Exception;

    /**
     * 删除文件
     *
     * @param source
     * @param fileNames
     * @throws Exception
     */
    boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception;

    /**
     * 路径目录是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isDirExist(ISourceDTO source, String remotePath) throws Exception;

    /**
     * 设置路径权限
     *
     * @param source
     * @param remotePath
     * @param mode
     * @throws Exception
     */
    boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception;

    /**
     * 重命名
     *
     * @param source
     * @param src
     * @param dist
     * @return
     * @throws Exception
     */
    boolean rename(ISourceDTO source, String src,String dist) throws Exception;

    /**
     * 复制文件
     *
     * @param source
     * @param src
     * @param dist
     * @param isOverwrite
     * @throws Exception
     */
    boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception;

    /**
     * 获取目录下所有文件
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception;

    /**
     * 获取目录下所有文件的属性集
     *
     * @param source
     * @param remotePath
     * @param isIterate 是否递归
     * @return
     * @throws Exception
     */
    List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception;

    /**
     * 从hdfs copy文件到本地
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @throws Exception
     */
    boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception;

    /**
     * 从本地copy到hdfs
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @param overwrite 是否重写
     * @throws Exception
     */
    boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception;

    /**
     * 从hdfs copy 到hdfs
     *
     * @param source1
     * @param source2
     * @param srcPath
     * @param dstPath
     * @param localPath
     * @param overwrite
     * @throws Exception
     */
    boolean copyInHDFS(ISourceDTO source1, ISourceDTO source2, String srcPath, String dstPath, String localPath, boolean overwrite) throws Exception;


    /**
     * 根据文件类型获取hdfsWriter
     *
     * @param fileFormat
     * @return
     * @throws Exception
     */
    IHdfsWriter getHdfsWriter(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception;

    /**
     * 根据文件格式获取对应的downlaoder
     * @param fileFormat
     * @return
     * @throws Exception
     */
    IDownloader getDownloaderByFormat(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception;

}
