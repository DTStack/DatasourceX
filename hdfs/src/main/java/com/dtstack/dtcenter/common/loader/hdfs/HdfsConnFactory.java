package com.dtstack.dtcenter.common.loader.hdfs;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/2/27
 * @Description：HDFS 连接工厂
 */
public class HdfsConnFactory extends ConnFactory {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Connection getConn(ISourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean testConn(ISourceDTO iSource) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        if (!hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }

        //kerberos认证
        if (MapUtils.isNotEmpty(hdfsSourceDTO.getKerberosConfig())) {
            DtKerberosUtils.loginKerberos(hdfsSourceDTO.getKerberosConfig());
        }
        Properties properties = combineHdfsConfig(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
        if (properties.size() > 0) {
            Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), properties);
            //不在做重复认证
            conf.set("hadoop.security.authorization", "false");
            //必须添加
            conf.set("dfs.namenode.kerberos.principal.pattern", "*");
            return HdfsOperator.checkConnection(conf);
        }
        return false;
    }

    /**
     * 高可用配置
     *
     * @param hadoopConfig
     * @param confMap
     * @return
     */
    private Properties combineHdfsConfig(String hadoopConfig, Map<String, Object> confMap) {
        Properties properties = new Properties();
        if (StringUtils.isNotBlank(hadoopConfig)) {
            try {
                properties = objectMapper.readValue(hadoopConfig, Properties.class);
            } catch (IOException e) {
                throw new DtCenterDefException("高可用配置格式错误", e);
            }
        }
        if (confMap != null) {
            for (String key : confMap.keySet()) {
                properties.setProperty(key, confMap.get(key).toString());
            }
        }
        return properties;
    }
}
