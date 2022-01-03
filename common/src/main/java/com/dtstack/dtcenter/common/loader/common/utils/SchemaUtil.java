package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * schema 工具
 *
 * @author ：wangchuan
 * date：Created in 下午3:31 2022/1/3
 * company: www.dtstack.com
 */
public class SchemaUtil {

    /**
     * 获取 schema，优先从 SqlQueryDTO 中获取
     *
     * @param sourceDTO   数据源连接信息
     * @param sqlQueryDTO 查询条件
     * @return schema 信息
     */
    public static String getSchema(ISourceDTO sourceDTO, SqlQueryDTO sqlQueryDTO) {
        AssertUtils.notNull(sourceDTO, "sourceDTO can't be null.");
        if (sourceDTO instanceof RdbmsSourceDTO) {
            if (Objects.isNull(sqlQueryDTO)) {
                return ((RdbmsSourceDTO) sourceDTO).getSchema();
            } else {
                return StringUtils.isNotBlank(sqlQueryDTO.getSchema()) ?
                        sqlQueryDTO.getSchema() : ((RdbmsSourceDTO) sourceDTO).getSchema();
            }
        }
        return Objects.isNull(sqlQueryDTO) ? null : sqlQueryDTO.getSchema();
    }
}
