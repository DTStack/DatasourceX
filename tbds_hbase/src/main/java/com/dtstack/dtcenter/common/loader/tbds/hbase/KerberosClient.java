package com.dtstack.dtcenter.common.loader.tbds.hbase;

import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:28 2020/9/9
 * @Description：Hbase Kerberos 服务
 */
@Slf4j
public class KerberosClient extends AbsKerberosClient {
    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        super.prepareKerberosForConnect(conf, localKerberosPath);
        // 设置principal账号
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            String principal = getPrincipals(conf).get(0);
            log.info("setting principal 为 {}", principal);
            conf.put(HadoopConfTool.PRINCIPAL, principal);
        }
        return true;
    }
}
