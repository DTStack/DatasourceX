package com.dtstack.dtcenter.common.loader.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.JSONUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hbase.pool.HbasePoolManager;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
 * @Date ：Created in 20:00 2020/2/27
 * @Description：Hbase 连接工厂
 */
@Slf4j
public class HbaseConnFactory {
    public Boolean testConn(ISourceDTO iSource) {
        HbaseSourceDTO hbaseSourceDTO = (HbaseSourceDTO) iSource;
        boolean check = false;
        Connection hConn = null;
        try {
            hConn = getHbaseConn(hbaseSourceDTO, null);
            hConn.getAdmin().getClusterStatus();
            check = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if ((hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) && hConn != null) {
                try {
                    hConn.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return check;
    }

    public static Connection getHbaseConn(HbaseSourceDTO source, SqlQueryDTO queryDTO) {
        if (source.getPoolConfig() == null || MapUtils.isNotEmpty(source.getKerberosConfig())) {
            return HbasePoolManager.initHbaseConn(source, queryDTO);
        }
        return HbasePoolManager.getConnection(source, queryDTO);
    }
}
