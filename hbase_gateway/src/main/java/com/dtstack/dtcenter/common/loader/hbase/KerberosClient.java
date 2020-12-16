package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
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
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath)  {
        super.prepareKerberosForConnect(conf, localKerberosPath);
        // 因为 HBASE 需要两张 Principal 所以这边做一下处理
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            String principal = getPrincipals(conf).get(0);
            log.info("手动设置 principal 为 {}", principal);
            conf.put(HadoopConfTool.PRINCIPAL, principal);
        }
        if (!conf.containsKey(HadoopConfTool.HBASE_MASTER_PRINCIPAL)) {
            throw new DtLoaderException(String.format("HBASE 需要配置 %s 的值", HadoopConfTool.HBASE_MASTER_PRINCIPAL));
        }

        if (!conf.containsKey(HadoopConfTool.HBASE_REGION_PRINCIPAL)) {
            log.info("手动设置 hbase.regionserver.kerberos.principal 为 {}", conf.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
            conf.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, conf.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
        }

        return true;
    }
}
