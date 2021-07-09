package com.dtstack.dtcenter.common.loader.doris;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlTableClient;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @author ：qianyi
 * date：Created in 下午1:46 2021/07/09
 * company: www.dtstack.com
 */
@Slf4j
public class DorisTableClient extends MysqlTableClient {

    /**
     * Alter Table 只支持三种操作类型:partition、rollup和schema change
     */
    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        throw new DtLoaderException("Method not support");
    }
}
