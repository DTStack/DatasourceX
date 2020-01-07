package com.dtstack.dtcenter.loader.rdbms.client;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.service.IRdbmsClient;

import java.sql.Connection;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:19 2020/1/6
 * @Description 代理实现
 */
public class RdbmsClientProxy implements IRdbmsClient {
    private IRdbmsClient targetClient;

    public RdbmsClientProxy(IRdbmsClient targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Connection getCon(String url, Properties prop) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<Connection>() {

                @Override
                public Connection execute() throws Exception {
                    return targetClient.getCon(url, prop);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean testCon(String url, Properties prop) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(new ClassLoaderCallBack<Boolean>() {

                @Override
                public Boolean execute() throws Exception {
                    return targetClient.testCon(url, prop);
                }
            }, targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }
}
