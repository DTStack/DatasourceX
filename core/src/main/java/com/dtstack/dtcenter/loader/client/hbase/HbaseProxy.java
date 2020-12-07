package com.dtstack.dtcenter.loader.client.hbase;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IHbase;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 * hbase客户端代理
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public class HbaseProxy implements IHbase {
    IHbase targetClient = null;

    public HbaseProxy(IHbase hbase) {
        this.targetClient = hbase;
    }

    @Override
    public Boolean isDbExists(ISourceDTO source, String namespace) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.isDbExists(source, namespace),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String tableName, String[] colFamily) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createHbaseTable(source, tableName, colFamily),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean createHbaseTable(ISourceDTO source, String namespace, String tableName, String[] colFamily) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createHbaseTable(source, namespace, tableName, colFamily),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> scanByRegex(ISourceDTO source, String tableName, String regex) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.scanByRegex(source, tableName, regex),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean deleteByRowKey(ISourceDTO source, String tableName, String family, String qualifier, List<String> rowKeys) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.deleteByRowKey(source, tableName, family, qualifier, rowKeys),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean putRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier, String data) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.putRow(source, tableName, rowKey, family, qualifier, data),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getRow(source, tableName, rowKey, family, qualifier),
                targetClient.getClass().getClassLoader());
    }

}
