package com.dtstack.dtcenter.common.loader.kerberos;

import com.dtstack.dtcenter.loader.client.IKerberos;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:32 2020/8/26
 * @Description：Kerberos 服务客户端
 */
public class KerberosClient implements IKerberos {
    @Override
    public Map<String, String> parseKerberosFromUpload(String zipLocation, String localKerberosPath, Integer datasourceType) throws Exception {
        return null;
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) throws Exception {
        return null;
    }

    @Override
    public String getPrincipal(String url, Integer datasourceType) throws Exception {
        return null;
    }

    @Override
    public List<String> getPrincipal(Map<String, Object> kerberosConfig) throws Exception {
        return null;
    }
}
