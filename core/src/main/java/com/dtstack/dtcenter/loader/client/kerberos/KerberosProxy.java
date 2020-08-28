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
    public Map<String, String> parseKerberosFromUpload(String zipLocation, String localKerberosPath, Integer datasourceType) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.parseKerberosFromUpload(zipLocation, localKerberosPath, datasourceType),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath, Integer datasourceType) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.prepareKerberosForConnect(conf, localKerberosPath, datasourceType),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public String getPrincipal(String url, Integer datasourceType) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipal(url, datasourceType),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<String> getPrincipal(Map<String, Object> kerberosConfig, Integer datasourceType) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipal(kerberosConfig, datasourceType),
                targetClient.getClass().getClassLoader(), true);
    }
}
