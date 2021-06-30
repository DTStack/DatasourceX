package com.dtstack.dtcenter.common.loader.opentsdb.tsdb;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;
import lombok.extern.slf4j.Slf4j;

/**
 * Open TSDB 连接工厂
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
@Slf4j
public class OpenTSDBConnFactory {

    /**
     * 获取 openTSDB Client，暂不支持连接池
     *
     * @param source 数据源连接信息
     * @return TSDB Client
     */
    public static TSDB getOpenTSDBClient(ISourceDTO source) {
        OpenTSDBSourceDTO openTSDBSourceDTO = (OpenTSDBSourceDTO) source;
        return new TSDBClient(openTSDBSourceDTO);
    }
}
