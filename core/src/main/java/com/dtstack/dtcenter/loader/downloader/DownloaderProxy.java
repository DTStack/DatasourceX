package com.dtstack.dtcenter.loader.downloader;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.List;

/**
 *
 * IDownloader代理类
 * 若不加此代理类，由于获取到IDownloader后，当前线程的ContextClassLoader会被设置成旧的ClassLoader
 * 这样在执行IDownloader中方法的时候如果方法中有对线程ContextClassLoader进行获取操作则会导致ClassLoader不一致
 *
 * @author ：wangchuan
 * date：Created in 下午1:15 2020/8/12
 * company: www.dtstack.com
 */
public class DownloaderProxy implements IDownloader {

    private IDownloader targetDownloader;

    public DownloaderProxy(IDownloader targetDownloader){
        this.targetDownloader = targetDownloader;
    }

    @Override
    public boolean configure() throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.configure(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getMetaInfo() throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.getMetaInfo(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Object readNext() throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.readNext(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean reachedEnd() throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.reachedEnd(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public boolean close() throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.close(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public String getFileName() {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetDownloader.getFileName(),
                    targetDownloader.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
