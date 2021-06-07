package com.dtstack.dtcenter.common.loader.influxdb;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.InfluxDBSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

/**
 * influxDB 连接工厂
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
@Slf4j
public class InfluxDBConnFactory {

    // 异常匹配器
    private static final IErrorPattern ERROR_PATTERN = new InfluxDBErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    /**
     * 获取 influxDB 连接客户端
     *
     * @param sourceDTO influxDB 数据源连接信息
     * @return influxDB 连接客户端
     */
    public static InfluxDB getClient(ISourceDTO sourceDTO) {
        InfluxDBSourceDTO influxDBSourceDTO = (InfluxDBSourceDTO) sourceDTO;
        InfluxDB influxDB;
        if (StringUtils.isNotBlank(influxDBSourceDTO.getUsername())) {
            influxDB = InfluxDBFactory.connect(influxDBSourceDTO.getUrl(), influxDBSourceDTO.getUsername(), influxDBSourceDTO.getPassword());
        } else {
            influxDB = InfluxDBFactory.connect(influxDBSourceDTO.getUrl());
        }
        // 设置 db
        if (StringUtils.isNotBlank(influxDBSourceDTO.getDatabase())) {
            influxDB.setDatabase(influxDBSourceDTO.getDatabase());
        }
        // 设置数据存储策略
        influxDB.setRetentionPolicy(influxDBSourceDTO.getRetentionPolicy());
        return influxDB;
    }

    /**
     * 测试 influxDB 连通性
     *
     * @param source 数据源连接信息
     * @return 是否连通
     */
    public static Boolean testCon(ISourceDTO source) {
        try (InfluxDB influxDB = InfluxDBConnFactory.getClient(source)) {
            Pong response = influxDB.ping();
            return response.isGood();
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        }
    }
}
