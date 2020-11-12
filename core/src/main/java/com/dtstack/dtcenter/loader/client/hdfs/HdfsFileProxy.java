package com.dtstack.dtcenter.loader.client.hdfs;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.downloader.DownloaderProxy;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:40 2020/8/10
 * @Description：Hdfs 代理
 */
@Slf4j
public class HdfsFileProxy implements IHdfsFile {

    private IHdfsFile targetClient;

    public HdfsFileProxy(IHdfsFile targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public FileStatus getStatus(ISourceDTO source, String location)  {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getStatus(source, location),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getLogDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            //这里返回给上层的是downLoader代理类
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getLogDownloader(source, queryDTO)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception {
        try {
           return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.downloadFileFromHdfs(source, remotePath, localDir),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.uploadLocalFileToHdfs(source, localFilePath, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.uploadInputStreamToHdfs(source, bytes, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createDir(source, remotePath, permission),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isFileExist(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.checkAndDelete(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getDirSize(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteFiles(source, fileNames),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isDirExist(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.setPermission(source, remotePath, mode),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.rename(source, src, dist),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyFile(source, src, dist, isOverwrite),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listAllFilePath(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listAllFiles(source, remotePath, isIterate),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyToLocal(source, srcPath, dstPath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyFromLocal(source, srcPath, dstPath, overwrite),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, String fieldDelimiter, String fileFormat) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloaderByFormat(source, tableLocation, fieldDelimiter, fileFormat)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnList(source, queryDTO, fileFormat),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.writeByPos(source, hdfsWriterDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.writeByName(source, hdfsWriterDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
