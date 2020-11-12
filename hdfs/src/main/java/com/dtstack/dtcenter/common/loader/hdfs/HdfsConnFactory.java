package com.dtstack.dtcenter.common.loader.hdfs;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.common.loader.hdfs.util.KerberosUtil;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.security.PrivilegedAction;
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
    @Override
    public Connection getConn(ISourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean testConn(ISourceDTO iSource) {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        if (StringUtils.isBlank(hdfsSourceDTO.getDefaultFS()) || !hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }

        Properties properties = HdfsConnFactory.combineHdfsConfig(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
        Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), properties);
        //不在做重复认证 主要用于 HdfsOperator.checkConnection 中有一些数栈自己的逻辑
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            return HdfsOperator.checkConnection(conf);
        }

        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> HdfsOperator.checkConnection(conf)
        );
    }

    /**
     * 高可用配置
     *
     * @param hadoopConfig
     * @param confMap
     * @return
     */
    public static Properties combineHdfsConfig(String hadoopConfig, Map<String, Object> confMap) {
        Properties properties = new Properties();
        if (StringUtils.isNotBlank(hadoopConfig)) {
            try {
                Map<String, Object> hadoopMap = JSONObject.parseObject(hadoopConfig);
                properties.putAll(hadoopMap);
            } catch (Exception e) {
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
