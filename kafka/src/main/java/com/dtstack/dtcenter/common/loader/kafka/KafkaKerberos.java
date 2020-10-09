package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

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
    protected void dealFile(List<File> unzipFileList, String localKerberosPath, Map<String, Object> confMap) throws IOException {
        // 设置 keytab
        String finalPath = KerberosConfigUtil.dealFilePath(unzipFileList, localKerberosPath, DtClassConsistent.PublicConsistent.KEYTAB_SUFFIX);
        log.info("DealKeytab path -- key : {}, value : {}", HadoopConfTool.PRINCIPAL_FILE, finalPath);
        confMap.put(HadoopConfTool.PRINCIPAL_FILE, finalPath);

        // 设置 Krb5
        KerberosConfigUtil.dealKrb5Conf(unzipFileList, localKerberosPath, confMap);
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        // 处理 Kafka Principal File，兼容历史数据
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL_FILE)) {
            if (conf.containsKey(HadoopConfTool.KAFKA_KERBEROS_KEYTAB)) {
                conf.put(HadoopConfTool.PRINCIPAL_FILE, MapUtils.getString(conf, HadoopConfTool.KAFKA_KERBEROS_KEYTAB));
            }
        }
        // 替换相对路径
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.PRINCIPAL_FILE);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);

        // 处理 Kafka Principal，兼容历史数据
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            if (conf.containsKey(HadoopConfTool.KAFKA_KERBEROS_SERVICE_NAME)) {
                conf.put(HadoopConfTool.PRINCIPAL, MapUtils.getString(conf, HadoopConfTool.KAFKA_KERBEROS_SERVICE_NAME));
            } else {
                List<String> principals = getPrincipals(conf);
                conf.put(HadoopConfTool.PRINCIPAL, principals.get(0));
            }
        }
        return true;
    }

    @Override
    public List<String> getPrincipals(Map<String, Object> kerberosConfig) {
        String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        if (StringUtils.isBlank(keytabPath)) {
            throw new DtLoaderException("获取 Principal 信息异常，Keytab 配置不存在");
        }
        return KerberosConfigUtil.getPrincipals(keytabPath);
    }
}
