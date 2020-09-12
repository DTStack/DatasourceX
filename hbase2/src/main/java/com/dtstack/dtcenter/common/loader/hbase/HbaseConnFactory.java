package com.dtstack.dtcenter.common.loader.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.JSONUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:08 2020/7/9
 * @Description：Hbase 连接工厂
 */
@Slf4j
public class HbaseConnFactory {
    public Boolean testConn(ISourceDTO iSource) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        boolean check = false;
        Connection hConn = null;
        try {
            hConn = getHbaseConn(hbaseSourceDTO);
            ClusterStatus clusterStatus = hConn.getAdmin().getClusterStatus();
            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (hConn != null) {
                    hConn.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        return check;
    }

    public static Connection getHbaseConn(HbaseSourceDTO source) throws Exception {
        Map<String, Object> sourceToMap = sourceToMap(source);
        if (MapUtils.isEmpty(source.getKerberosConfig())) {
            Configuration hConfig = HBaseConfiguration.create();
            for (Map.Entry<String, Object> entry : sourceToMap.entrySet()) {
                hConfig.set(entry.getKey(), (String) entry.getValue());
            }

            Connection hConn = null;
            try {
                hConn = ConnectionFactory.createConnection(hConfig);
                return hConn;
            } catch (Exception e) {
                throw new DtLoaderException("获取 hbase 连接异常", e);
            }
        }

        return KerberosLoginUtil.loginKerberosWithUGI(new HashMap<>(source.getKerberosConfig())).doAs(
                (PrivilegedAction<Connection>) () -> {
                    Configuration hConfig = HBaseConfiguration.create();
                    for (Map.Entry<String, Object> entry : sourceToMap.entrySet()) {
                        hConfig.set(entry.getKey(), (String) entry.getValue());
                    }

                    Connection hConn = null;
                    try {
                        hConn = ConnectionFactory.createConnection(hConfig);
                        return hConn;
                    } catch (Exception e) {
                        throw new DtLoaderException("获取 hbase 连接异常", e);
                    }
                }
        );
    }

    /**
     * 数据源 改成 HBase 需要的 Map 信息
     *
     * @param iSource
     * @return
     */
    private static Map<String, Object> sourceToMap(ISourceDTO iSource) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        Map<String, Object> hbaseMap = new HashMap<>();
        //对于直接传config的 走直接生成的逻辑

        if (StringUtils.isNotBlank(hbaseSourceDTO.getConfig())) {
            JSONObject jsonObject = JSON.parseObject(hbaseSourceDTO.getConfig());
            hbaseMap.putAll(jsonObject);
        } else {

            if (StringUtils.isBlank(hbaseSourceDTO.getUrl())) {
                throw new DtLoaderException("集群地址不能为空");
            }
            // 设置集群地址
            hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSourceDTO.getUrl());

            // 设置根路径
            if (StringUtils.isNotBlank(hbaseSourceDTO.getPath())) {
                hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_ZOOKEEPER_ZNODE_PARENT, hbaseSourceDTO.getPath());
            }

        }

        // 设置其他信息
        hbaseMap.putAll(JSONUtil.parseMap(hbaseSourceDTO.getOthers()));

        // 设置 Kerberos 信息
        if (MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) {
            hbaseMap.putAll(hbaseSourceDTO.getKerberosConfig());
            hbaseMap.put("hadoop.security.authentication", "Kerberos");
            hbaseMap.put("hbase.security.authentication", "Kerberos");
            hbaseMap.put("hbase.master.kerberos.principal", hbaseMap.get("hbase.master.kerberos.principal"));
            log.info("getHbaseConnection principalFile:{}", hbaseMap.get("principalFile"));
        }

        // 设置默认信息
        hbaseMap.put("hbase.rpc.timeout", "60000");
        hbaseMap.put("ipc.socket.timeout", "20000");
        hbaseMap.put("hbase.client.retries.number", "3");
        hbaseMap.put("hbase.client.pause", "100");
        hbaseMap.put("zookeeper.recovery.retry", "3");
        return hbaseMap;
    }
}
