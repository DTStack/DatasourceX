package com.dtstack.dtcenter.common.loader.inceptor.client;

import com.dtstack.dtcenter.common.loader.inceptor.InceptorConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * inceptor table client
 *
 * @author ：wangchuan
 * date：Created in 下午2:19 2021/5/6
 * company: www.dtstack.com
 */
public class InceptorTableClient extends AbsTableClient {

    @Override
    protected ConnFactory getConnFactory() {
        return new InceptorConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.INCEPTOR;
    }

    private static final String DESC_FORMATTED_SQL = "desc formatted %s";

    @Override
    public Boolean isView(ISourceDTO source, String schema, String tableName) {
        checkParamAndSetSchema(source, schema, tableName);
        String sql = String.format(DESC_FORMATTED_SQL, tableName);
        List<Map<String, Object>> result = executeQuery(source, sql);
        if (CollectionUtils.isEmpty(result)) {
            throw new DtLoaderException(String.format("Execute to determine whether the table is a view sql result is empty，sql：%s", sql));
        }
        String tableType = "";
        for (Map<String, Object> row : result) {
            String colName = MapUtils.getString(row, "category", "");
            if (StringUtils.containsIgnoreCase(colName, "Type")) {
                tableType = MapUtils.getString(row, "attribute");
                break;
            }
        }
        return StringUtils.containsIgnoreCase(tableType, "VIEW");
    }
}
