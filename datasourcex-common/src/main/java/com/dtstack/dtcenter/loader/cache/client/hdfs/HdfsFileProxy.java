/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.cache.client.hdfs;

import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.downloader.DownloaderProxy;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSContentSummary;
import com.dtstack.dtcenter.loader.dto.HdfsQueryDTO;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.client.IDownloader;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

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
    public FileStatus getStatus(ISourceDTO source, String location) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getStatus(source, location),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getLogDownloader(ISourceDTO source, SqlQueryDTO queryDTO) {
        try {
            //这里返回给上层的是downLoader代理类
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getLogDownloader(source, queryDTO)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getTaskManagerList(ISourceDTO source, String submitUser, String appId) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTaskManagerList(source, submitUser, appId),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getFileDownloader(ISourceDTO source, String path) {
        try {
            //这里返回给上层的是downLoader代理类
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getFileDownloader(source, path)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.downloadFileFromHdfs(source, remotePath, localDir),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.uploadLocalFileToHdfs(source, localFilePath, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.uploadInputStreamToHdfs(source, bytes, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String uploadStringToHdfs(ISourceDTO source, String str, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.uploadStringToHdfs(source, str, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createDir(source, remotePath, permission),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isFileExist(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.checkAndDelete(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean delete(ISourceDTO source, String remotePath, boolean recursive) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.delete(source, remotePath, recursive),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getDirSize(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteFiles(source, fileNames),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isDirExist(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.setPermission(source, remotePath, mode),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.rename(source, src, dist),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyFile(source, src, dist, isOverwrite),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyDirector(ISourceDTO source, String src, String dist) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyDirector(source, src, dist),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean fileMerge(ISourceDTO source, String src, String mergePath, FileFormat fileFormat, Long maxCombinedFileSize, Long needCombineFileSizeLimit) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.fileMerge(source, src, mergePath, fileFormat, maxCombinedFileSize, needCombineFileSizeLimit),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatus> listStatus(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listStatus(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listAllFilePath(source, remotePath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.listAllFiles(source, remotePath, isIterate),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyToLocal(source, srcPath, dstPath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.copyFromLocal(source, srcPath, dstPath, overwrite),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloaderByFormat(source, tableLocation, columnNames, fieldDelimiter, fileFormat)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getDownloaderByFormatWithType(ISourceDTO source, String tableLocation, List<ColumnMetaDTO> allColumns, List<String> filterColumns, Map<String, String> filterPartition, List<String> partitions, String fieldDelimiter, String fileFormat) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloaderByFormatWithType(source, tableLocation, allColumns, filterColumns, filterPartition, partitions, fieldDelimiter, fileFormat)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getDownloaderByFormatWithType(ISourceDTO source, String tableLocation, List<ColumnMetaDTO> allColumns, List<String> filterColumns, Map<String, String> filterPartition, List<String> partitions, String fieldDelimiter, String fileFormat, Boolean isTransTable) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> new DownloaderProxy(targetClient.getDownloaderByFormatWithType(source, tableLocation, allColumns, filterColumns, filterPartition, partitions, fieldDelimiter, fileFormat, isTransTable)),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnList(source, queryDTO, fileFormat),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.writeByPos(source, hdfsWriterDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.writeByName(source, hdfsWriterDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<HDFSContentSummary> getContentSummary(ISourceDTO source, List<String> hdfsDirPaths) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getContentSummary(source, hdfsDirPaths),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public HDFSContentSummary getContentSummary(ISourceDTO source, String hdfsDirPath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getContentSummary(source, hdfsDirPath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String getHdfsWithScript(ISourceDTO source, String hdfsPath) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getHdfsWithScript(source, hdfsPath),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getHdfsWithJob(ISourceDTO source, HdfsQueryDTO hdfsQueryDTO) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getHdfsWithJob(source, hdfsQueryDTO),
                    targetClient.getClass().getClassLoader());
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
