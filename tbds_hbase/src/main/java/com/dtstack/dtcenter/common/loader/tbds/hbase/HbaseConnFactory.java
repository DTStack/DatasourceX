package com.dtstack.dtcenter.common.loader.tbds.hbase;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.tbds.hbase.pool.HbasePoolManager;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.TbdsHbaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:00 2020/2/27
 * @Description：Hbase 连接工厂
 */
@Slf4j
public class HbaseConnFactory {

    // 连接超时时间 单位：秒。 默认60秒
    private static final Integer TIMEOUT = 60;

    private static final IErrorPattern ERROR_PATTERN = new HbaseErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    public Boolean testConn(ISourceDTO iSource) {
        TbdsHbaseSourceDTO hbaseSourceDTO = (TbdsHbaseSourceDTO) iSource;
        boolean check = false;
        Connection hConn = null;
        try {
            hConn = getHbaseConn(hbaseSourceDTO, SqlQueryDTO.builder().build());
            hConn.getAdmin().getClusterStatus();
            check = true;
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        } finally {
            if ((hbaseSourceDTO.getPoolConfig() == null || MapUtils.isNotEmpty(hbaseSourceDTO.getKerberosConfig())) && hConn != null) {
                try {
                    hConn.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
            HbaseClient.destroyProperty();
        }
        return check;
    }

    public static Connection getHbaseConn(TbdsHbaseSourceDTO source, SqlQueryDTO queryDTO) {
        if (source.getPoolConfig() == null || MapUtils.isNotEmpty(source.getKerberosConfig())) {
            return HbasePoolManager.initHbaseConn(source, queryDTO);
        }
        return HbasePoolManager.getConnection(source, queryDTO);
    }

    public static Connection getHbaseConn(TbdsHbaseSourceDTO source, Integer queryTimeout) {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().queryTimeout(queryTimeout).build();
        return getHbaseConn(source, queryDTO);
    }

    public static Connection getHbaseConn(TbdsHbaseSourceDTO source) {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().queryTimeout(TIMEOUT).build();
        return getHbaseConn(source, queryDTO);
    }
}
