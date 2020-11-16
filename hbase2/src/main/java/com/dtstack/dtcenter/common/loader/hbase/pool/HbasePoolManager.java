package com.dtstack.dtcenter.common.loader.hbase.pool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.JSONUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2020-10-31 14:37
 * @Description:
 */
@Slf4j
public class HbasePoolManager {

    private volatile static HbasePoolManager manager;

    private volatile static Map<String, Connection> sourcePool = Maps.newConcurrentMap();

    private static final String HBASE_KEY = "zookeeperUrl:%s,zNode:%s";

    private HbasePoolManager() {
    }

    public static HbasePoolManager getInstance() {
        if (null == manager) {
            synchronized (HbasePoolManager.class) {
                if (null == manager) {
                    manager = new HbasePoolManager();
                }
            }
        }
        return manager;
    }

    public static Connection getConnection(ISourceDTO source, SqlQueryDTO queryDTO) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) source;
        String key = getPrimaryKey(hbaseSourceDTO).intern();
        Connection conn = sourcePool.get(key);
        if (conn == null) {
            synchronized (HbasePoolManager.class) {
                conn = sourcePool.get(key);
                if (conn == null) {
                    conn = initHbaseConn(hbaseSourceDTO, queryDTO);
                    sourcePool.putIfAbsent(key, conn);
                }
            }
        }
        return conn;
    }

    private static String getPrimaryKey(HbaseSourceDTO hbaseSourceDTO) {
        return String.format(HBASE_KEY, hbaseSourceDTO.getUrl(), hbaseSourceDTO.getPath());
    }

    public static Connection initHbaseConn(HbaseSourceDTO source, SqlQueryDTO queryDTO) {
        Map<String, Object> sourceToMap = sourceToMap(source, queryDTO);
        Configuration hConfig = HBaseConfiguration.create();
        for (Map.Entry<String, Object> entry : sourceToMap.entrySet()) {
            hConfig.set(entry.getKey(), (String) entry.getValue());
        }
        return KerberosLoginUtil.loginWithUGI(source.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        return ConnectionFactory.createConnection(hConfig);
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
    private static Map<String, Object> sourceToMap(ISourceDTO iSource, SqlQueryDTO queryDTO) {
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

        // 设置 Kerberos 信息
        if (MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) {
            hbaseMap.putAll(hbaseSourceDTO.getKerberosConfig());
            hbaseMap.put("hadoop.security.authentication", "Kerberos");
            hbaseMap.put("hbase.security.authentication", "Kerberos");
            hbaseMap.put("hbase.master.kerberos.principal", hbaseMap.get("hbase.master.kerberos.principal"));
            log.info("getHbaseConnection principalFile:{}", hbaseMap.get("principalFile"));
        }

        // 设置默认信息
        hbaseMap.put("hbase.rpc.timeout", queryDTO == null || queryDTO.getQueryTimeout() == null ? "60000" : String.valueOf(queryDTO.getQueryTimeout() * 1000));
        hbaseMap.put("ipc.socket.timeout", "20000");
        hbaseMap.put("hbase.client.retries.number", "3");
        hbaseMap.put("hbase.client.pause", "100");
        hbaseMap.put("zookeeper.recovery.retry", "3");
        hbaseMap.put("hbase.client.ipc.pool.type","RoundRobinPool");
        Integer poolSize = hbaseSourceDTO.getPoolConfig().getMaximumPoolSize();
        hbaseMap.put("hbase.client.ipc.pool.size", poolSize == null || poolSize == 0 ? "10" : String.valueOf(poolSize));

        // 设置其他信息
        hbaseMap.putAll(JSONUtil.parseMap(hbaseSourceDTO.getOthers()));
        return hbaseMap;
    }

    @PreDestroy
    public void doDestroy() {
        for (Map.Entry<String, Connection> entry : sourcePool.entrySet()) {
            Connection connection = entry.getValue();
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    throw new DtLoaderException("hbase连接关闭失败", e);
                }
            }
        }
    }


}
