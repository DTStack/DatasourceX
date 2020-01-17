package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:19 2020/1/6
 * @Description 代理实现
 */
public class DataSourceClientProxy implements IClient {
    private IClient targetClient;

    public DataSourceClientProxy(IClient targetClient) {
        this.targetClient = targetClient;
    }
    
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<Connection>() {

                @Override
                public Connection execute() throws Exception {
                    return targetClient.getCon(source);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean testCon(SourceDTO source) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<Boolean>() {

                @Override
                public Boolean execute() throws Exception {
                    return targetClient.testCon(source);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(Connection conn, String sql) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<List>() {

                @Override
                public List execute() throws Exception {
                    return targetClient.executeQuery(conn, sql);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, String sql) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<List>() {

                @Override
                public List<Map<String, Object>> execute() throws Exception {
                    return targetClient.executeQuery(source, sql);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }
}
