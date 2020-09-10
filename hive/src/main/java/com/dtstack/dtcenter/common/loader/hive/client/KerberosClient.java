package com.dtstack.dtcenter.common.loader.hive.client;

import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:13 2020/9/8
 * @Description：Hive Kerberos 操作
 */
public class KerberosClient extends AbsKerberosClient {
    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) throws Exception {
        super.prepareKerberosForConnect(conf, localKerberosPath);
        // 因为 Hive 需要下载所以设置 ResourceManager Principal,没有用 getOrDefault 主要是为了减少一次 keytab 获取 Principal 的过程
        if (conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            conf.put(HadoopConfTool.RM_PRINCIPAL, conf.get(HadoopConfTool.PRINCIPAL));
        } else {
            conf.put(HadoopConfTool.RM_PRINCIPAL, getPrincipals(conf).get(0));
        }

        return true;
    }
}
