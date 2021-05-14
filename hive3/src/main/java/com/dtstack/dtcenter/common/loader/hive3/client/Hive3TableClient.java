package com.dtstack.dtcenter.common.loader.hive3.client;

import com.dtstack.dtcenter.common.loader.hive3.HiveConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * hive表操作相关接口
 *
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 14:03 2021/05/13
 * @Description：Hive3
 */
@Slf4j
public class Hive3TableClient extends AbsTableClient {

    @Override
    protected ConnFactory getConnFactory() {
        return new HiveConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.HIVE;
    }

    private static final String TABLE_IS_VIEW_SQL = "desc formatted %s";

    @Override
    public Boolean isView(ISourceDTO source, String schema, String tableName) {
        checkParamAndSetSchema(source, schema, tableName);
        String sql = String.format(TABLE_IS_VIEW_SQL, tableName);
        List<Map<String, Object>> result = executeQuery(source, sql);
        if (CollectionUtils.isEmpty(result)) {
            throw new DtLoaderException(String.format("Execute to determine whether the table is a view sql result is empty，sql：%s", sql));
        }
        String tableType = "";
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "col_name");
            if (StringUtils.containsIgnoreCase(colName, "Type")) {
                tableType = MapUtils.getString(row, "data_type");
                break;
            }
        }
        return StringUtils.containsIgnoreCase(tableType, "VIEW");
    }
}
