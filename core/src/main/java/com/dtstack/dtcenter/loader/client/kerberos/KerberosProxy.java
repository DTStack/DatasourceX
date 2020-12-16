package com.dtstack.dtcenter.loader.client.kerberos;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKerberos;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:42 2020/8/26
 * @Description：Kerberos 实现类
 */
public class KerberosProxy implements IKerberos {
    IKerberos targetClient = null;

    public KerberosProxy(IKerberos kerberos) {
        this.targetClient = kerberos;
    }

    @Override
    public Map<String, Object> parseKerberosFromUpload(String zipLocation, String localKerberosPath) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.parseKerberosFromUpload(zipLocation, localKerberosPath),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.prepareKerberosForConnect(conf, localKerberosPath),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getPrincipals(String url) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(url),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getPrincipals(Map<String, Object> kerberosConfig) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(kerberosConfig),
                targetClient.getClass().getClassLoader());
    }
}
