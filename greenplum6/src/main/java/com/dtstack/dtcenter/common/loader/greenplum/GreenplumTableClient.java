package com.dtstack.dtcenter.common.loader.greenplum;

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
 * Greenplum表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class GreenplumTableClient extends AbsTableClient {

    @Override
    protected ConnFactory getConnFactory() {
        return new GreenplumFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.GREENPLUM6;
    }

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) throws Exception {
        throw new DtLoaderException("greenplum不支持获取分区操作！");
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) throws Exception {
        log.info("libra删除表，表名：{}", tableName);
        if (StringUtils.isBlank(tableName)) {
            throw new DtLoaderException("表名不能为空！");
        }
        String dropTableSql = String.format("drop table if exists %s", tableName);
        return executeSqlWithoutResultSet(source, dropTableSql);
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) throws Exception {
        throw new DtLoaderException("greenplum暂时不支持更改表参数操作！");
    }
}
