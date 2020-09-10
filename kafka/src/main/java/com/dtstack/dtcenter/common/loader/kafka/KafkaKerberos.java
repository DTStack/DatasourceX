package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:49 2020/9/1
 * @Description：Kafka Kerberos 操作
 */
@Slf4j
public class KafkaKerberos extends AbsKerberosClient {
    @Override
    protected void dealFile(List<File> unzipFileList, String localKerberosPath, Map<String, String> confMap) throws IOException {
        // 设置 keytab
        String finalPath = KerberosConfigUtil.dealFilePath(unzipFileList, localKerberosPath, DtClassConsistent.PublicConsistent.KEYTAB_SUFFIX);
        log.info("DealKeytab path -- key : {}, value : {}", HadoopConfTool.PRINCIPAL_FILE, finalPath);
        confMap.put(KafkaConsistent.KAFKA_KERBEROS_KEYTAB, finalPath);

        // 设置 Krb5
        KerberosConfigUtil.dealKrb5Conf(unzipFileList, localKerberosPath, confMap);
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) throws Exception {
        // 替换相对路径
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KAFKA_KERBEROS_KEYTAB);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);

        // 处理 Kafka Kerberos
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            List<String> principals = getPrincipals(conf);
            conf.put(HadoopConfTool.PRINCIPAL, principals);
        }

        return true;
    }
}
