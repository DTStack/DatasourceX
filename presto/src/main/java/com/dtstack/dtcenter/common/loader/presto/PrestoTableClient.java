package com.dtstack.dtcenter.common.loader.presto;

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
 * @author ：qianyi
 * company: www.dtstack.com
 */
@Slf4j
public class PrestoTableClient extends AbsTableClient {


    @Override
    protected ConnFactory getConnFactory() {
        return new PrestoConnFactory();
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

    @Override
    public Boolean renameTable(ISourceDTO source, String oldTableName, String newTableName) {
        throw new DtLoaderException("The method is not supported");
    }

    @Override
    public Boolean dropTable(ISourceDTO source, String tableName) {
        throw new DtLoaderException("The method is not supported");
    }


    /**
     * 添加表字段
     *
     * @param source
     * @param columnMetaDTO
     * @return
     */
    protected Boolean addTableColumn(ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String schema = StringUtils.isNotBlank(columnMetaDTO.getSchema()) ? columnMetaDTO.getSchema() : rdbmsSourceDTO.getSchema();
        String comment = StringUtils.isNotBlank(columnMetaDTO.getColumnComment()) ? columnMetaDTO.getColumnComment() : "";
        String sql = String.format(ADD_COLUMN_SQL, transferSchemaAndTableName(schema, columnMetaDTO.getTableName()), columnMetaDTO.getColumnName(), columnMetaDTO.getColumnType(), comment);
        return executeSqlWithoutResultSet(source, sql);
    }
}
