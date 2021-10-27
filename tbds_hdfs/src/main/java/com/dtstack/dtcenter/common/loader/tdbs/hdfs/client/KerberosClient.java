package com.dtstack.dtcenter.common.loader.tdbs.hdfs.client;

import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:08 2020/9/17
 * @Description：Hdfs Kerberos 操作
 */
public class KerberosClient extends AbsKerberosClient {
    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        // 替换相对路径
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.PRINCIPAL_FILE);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);

        // 处理 hdfs Principal，因为需要下载，所以放在此处处理添加 RM 参数
        String principal = !conf.containsKey(HadoopConfTool.PRINCIPAL) ? getPrincipals(conf).get(0) : MapUtils.getString(conf, HadoopConfTool.PRINCIPAL);
        conf.put(HadoopConfTool.PRINCIPAL, principal);
        conf.put(HadoopConfTool.RM_PRINCIPAL, principal);
        return true;
    }
}
