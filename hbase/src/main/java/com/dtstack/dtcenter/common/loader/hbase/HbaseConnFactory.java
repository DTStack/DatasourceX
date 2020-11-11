package com.dtstack.dtcenter.common.loader.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:00 2020/2/27
 * @Description：Hbase 连接工厂
 */
@Slf4j
public class HbaseConnFactory extends ConnFactory {
    @Override
    public Boolean testConn(ISourceDTO iSource) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        boolean check = false;
        org.apache.hadoop.hbase.client.Connection hConn = null;
        try {
            hConn = getHbaseConn(hbaseSourceDTO, null);
            hConn.getAdmin().getClusterStatus();
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

    public static org.apache.hadoop.hbase.client.Connection getHbaseConn(HbaseSourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Map<String, Object> sourceToMap = sourceToMap(source, queryDTO);
        return KerberosUtil.loginWithUGI(new HashMap<>(source.getKerberosConfig())).doAs(
                (PrivilegedAction<org.apache.hadoop.hbase.client.Connection>) () -> {
                    Configuration hConfig = HBaseConfiguration.create();
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
        );
    }

    /**
     * 数据源 改成 HBase 需要的 Map 信息
     *
     * @param iSource
     * @param queryDTO
     * @return
     */
    private static Map<String, Object> sourceToMap(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        Map<String, Object> hbaseMap = new HashMap<>();
        //对于直接传config的 走直接生成的逻辑

        if (StringUtils.isNotBlank(hbaseSourceDTO.getConfig())) {
            JSONObject jsonObject = JSON.parseObject(hbaseSourceDTO.getConfig());
            hbaseMap.putAll(jsonObject);
        } else {
            if (StringUtils.isBlank(hbaseSourceDTO.getUrl())) {
                throw new DtCenterDefException("集群地址不能为空");
            }
            // 设置集群地址
            hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_HBASE_ZOOKEEPER_QUORUM, hbaseSourceDTO.getUrl());

            // 设置根路径
            if (StringUtils.isNotBlank(hbaseSourceDTO.getPath())) {
                hbaseMap.put(DtClassConsistent.HBaseConsistent.KEY_ZOOKEEPER_ZNODE_PARENT, hbaseSourceDTO.getPath());
            }
        }

        // 设置 Kerberos 信息
        if (MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) {
            hbaseMap.putAll(hbaseSourceDTO.getKerberosConfig());
            hbaseMap.put("hadoop.security.authentication", "Kerberos");
            hbaseMap.put("hbase.security.authentication", "Kerberos");
            hbaseMap.put("hbase.master.kerberos.principal", hbaseMap.get("hbase.master.kerberos.principal"));
            hbaseMap.put("hbase.regionserver.kerberos.principal", hbaseMap.get("hbase.regionserver.kerberos.principal"));
            log.info("getHbaseConnection principalFile:{}", hbaseMap.get("principalFile"));
        }

        // 设置默认信息
        hbaseMap.put("hbase.rpc.timeout", queryDTO == null || queryDTO.getQueryTimeout() == null ? "60000" : String.valueOf(queryDTO.getQueryTimeout() * 1000));
        hbaseMap.put("ipc.socket.timeout", "20000");
        hbaseMap.put("hbase.client.retries.number", "3");
        hbaseMap.put("hbase.client.pause", "100");
        hbaseMap.put("zookeeper.recovery.retry", "3");

        // 设置其他信息
        hbaseMap.putAll(JSONUtil.parseMap(hbaseSourceDTO.getOthers()));
        return hbaseMap;
    }

    @Override
    public Connection getConn(ISourceDTO source) {
        throw new DtLoaderException("Not Support");
    }
}
