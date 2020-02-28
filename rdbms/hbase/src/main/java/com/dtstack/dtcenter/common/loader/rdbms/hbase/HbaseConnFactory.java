package com.dtstack.dtcenter.common.loader.rdbms.hbase;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.JSONUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:00 2020/2/27
 * @Description：Hbase 连接工厂
 */
public class HbaseConnFactory extends ConnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseConnFactory.class);
    @Override
    public Boolean testConn(SourceDTO source) {
        boolean check = false;
        org.apache.hadoop.hbase.client.Connection hConn = null;
        try {
            hConn = getHbaseConn(source);
            ClusterStatus clusterStatus = hConn.getAdmin().getClusterStatus();
            check = true;
        } catch(Exception e) {
            LOG.error("{}", e);
        } finally {
            try {
                hConn.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return check;
    }

    public static org.apache.hadoop.hbase.client.Connection getHbaseConn(SourceDTO source) throws Exception {
        if (MapUtils.isNotEmpty(source.getKerberosConfig())) {
            DtKerberosUtils.loginKerberos(source.getKerberosConfig());
        }

        Configuration hConfig = HBaseConfiguration.create();
        Map<String, Object> sourceToMap = sourceToMap(source);
        for (Map.Entry<String, Object> entry : sourceToMap.entrySet()) {
            hConfig.set(entry.getKey(), (String) entry.getValue());
        }

        org.apache.hadoop.hbase.client.Connection hConn = null;
        try {
            hConn = ConnectionFactory.createConnection(hConfig);
            return hConn;
        } catch (Exception e) {
            throw new RuntimeException("获取 hbase 连接异常", e);
        }
    }

    /**
     * 数据源 改成 HBase 需要的 Map 信息
     *
     * @param source
     * @return
     */
    private static Map<String, Object> sourceToMap(SourceDTO source) {
        if (StringUtils.isBlank(source.getUrl())) {
            throw new DtCenterDefException("集群地址不能为空");
        }

        Map<String, Object> hbaseMap = new HashMap<>();
        // 设置集群地址
        hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_HBASE_ZOOKEEPER_QUORUM, source.getUrl());

        // 设置根路径
        if (StringUtils.isNotBlank(source.getPath())) {
            hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_ZOOKEEPER_ZNODE_PARENT, source.getPath());
        }

        // 设置其他信息
        hbaseMap.putAll(JSONUtil.parseMap(source.getOthers()));

        // 设置 Kerberos 信息
        if (MapUtils.isNotEmpty(source.getKerberosConfig())) {
            hbaseMap.putAll(source.getKerberosConfig());
        }

        // 设置默认信息
        hbaseMap.put("hbase.rpc.timeout", "60000");
        hbaseMap.put("ipc.socket.timeout", "20000");
        hbaseMap.put("hbase.client.retries.number", "3");
        hbaseMap.put("hbase.client.pause", "100");
        hbaseMap.put("zookeeper.recovery.retry", "3");
        return hbaseMap;
    }

    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
