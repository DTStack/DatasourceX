package com.dtstack.dtcenter.common.loader.libra;

import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Libra表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class LibraTableClient extends AbsTableClient {


    @Override
    protected ConnFactory getConnFactory() {
        return new LibraConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.LIBRA;
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        throw new DtLoaderException("Libra不支持获取分区操作！");
    }

    @Override
    protected String getDropTableSql(String tableName) {
        return String.format("drop table if exists %s", tableName);
    }

    /**
     * 更改表相关参数，暂时只支持更改表注释
     * @param source 数据源信息
     * @param tableName 表名
     * @param params 修改的参数，map集合
     * @return 执行结果
     */
    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        String comment = params.get("comment");
        log.info("libra更改表注释，comment：{}！", comment);
        if (StringUtils.isEmpty(comment)) {
            return true;
        }
        String alterTableParamsSql = String.format("COMMENT ON TABLE %s IS '%s'", tableName, comment);
        return executeSqlWithoutResultSet(source, alterTableParamsSql);
    }
}
