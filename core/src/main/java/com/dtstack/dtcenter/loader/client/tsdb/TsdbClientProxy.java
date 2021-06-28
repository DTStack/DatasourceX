package com.dtstack.dtcenter.loader.client.tsdb;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.ITsdb;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.tsdb.QueryResult;
import com.dtstack.dtcenter.loader.dto.tsdb.Suggest;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbPoint;
import com.dtstack.dtcenter.loader.dto.tsdb.TsdbQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p> OpenTSDB 代理类</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2021/6/23
 * company: www.dtstack.com
 */
public class TsdbClientProxy implements ITsdb {

    ITsdb targetClient;

    public TsdbClientProxy(ITsdb tsdb) {
        this.targetClient = tsdb;
    }

    @Override
    public Boolean putSync(ISourceDTO source, Collection<TsdbPoint> points) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.putSync(source, points),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<QueryResult> query(ISourceDTO source, TsdbQuery query) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.query(source, query),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, long startTime, long endTime) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteData(source, metric, startTime, endTime),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, Map<String, String> tags, long startTime, long endTime) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteData(source, metric, tags, startTime, endTime),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, List<String> fields, long startTime, long endTime) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteData(source, metric, fields, startTime, endTime),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteData(ISourceDTO source, String metric, Map<String, String> tags, List<String> fields, long startTime, long endTime) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteData(source, metric, tags, fields, startTime, endTime),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteMeta(source, metric, tags),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags, List<String> fields) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteMeta(source, metric, tags, fields),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteMeta(source, metric, tags, deleteData, recursive),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteMeta(ISourceDTO source, String metric, List<String> fields, Map<String, String> tags, boolean deleteData, boolean recursive) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteMeta(source, metric, fields, tags, deleteData, recursive),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> suggest(ISourceDTO source, Suggest type, String prefix, int max) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.suggest(source, type, prefix, max),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> suggest(ISourceDTO source, Suggest type, String metric, String prefix, int max) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.suggest(source, type, metric, prefix, max),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String version(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.version(source),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Map<String, String> getVersionInfo(ISourceDTO source) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getVersionInfo(source),
                targetClient.getClass().getClassLoader());
    }
}
