package com.dtstack.dtcenter.loader.client.hdfs.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

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
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public IDownloader getLogDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getLogDownloader(source, queryDTO),
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
