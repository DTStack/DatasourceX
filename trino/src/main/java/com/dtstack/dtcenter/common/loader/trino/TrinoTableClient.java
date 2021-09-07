package com.dtstack.dtcenter.common.loader.trino;

import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * trino 数据源表操作客户端
 *
 * @author ：wangchuan
 * date：Created in 下午2:21 2021/9/9
 * company: www.dtstack.com
 */
@Slf4j
public class TrinoTableClient extends AbsTableClient {


    @Override
    protected ConnFactory getConnFactory() {
        return new TrinoConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Presto;
    }

    private static final String ADD_COLUMN_SQL = "alter table %s add column %s %s comment '%s'";

    @Override
    public List<String> showPartitions(ISourceDTO source, String tableName) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Boolean alterTableParams(ISourceDTO source, String tableName, Map<String, String> params) {
        throw new DtLoaderException("The method is not supported");
    }

    /**
     * 添加表字段
     *
     * @param source        数据源信息
     * @param columnMetaDTO 字段信息
     * @return 是否成功
     */
    protected Boolean addTableColumn(ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String schema = StringUtils.isNotBlank(columnMetaDTO.getSchema()) ? columnMetaDTO.getSchema() : rdbmsSourceDTO.getSchema();
        String comment = StringUtils.isNotBlank(columnMetaDTO.getColumnComment()) ? columnMetaDTO.getColumnComment() : "";
        String sql = String.format(ADD_COLUMN_SQL, transferSchemaAndTableName(schema, columnMetaDTO.getTableName()), columnMetaDTO.getColumnName(), columnMetaDTO.getColumnType(), comment);
        return executeSqlWithoutResultSet(source, sql);
    }
}
