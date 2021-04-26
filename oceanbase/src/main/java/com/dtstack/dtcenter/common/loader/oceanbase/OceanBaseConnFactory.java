package com.dtstack.dtcenter.common.loader.oceanbase;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OceanBaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 14:18 2021/4/21
 */
public class OceanBaseConnFactory extends ConnFactory {
    public OceanBaseConnFactory() {
        driverName = DataBaseType.OceanBase.getDriverClassName();
        this.errorPattern = new OceanBaseErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        Connection connection = super.getConn(iSource, taskParams);
        OceanBaseSourceDTO source = (OceanBaseSourceDTO) iSource;
        String schema = source.getSchema();
        if (StringUtils.isNotEmpty(schema)) {
            try (Statement statement = connection.createStatement()) {
                //选择schema
                String useSchema = String.format("USE %s", schema);
                statement.execute(useSchema);
            } catch (Exception e) {
                throw new DtLoaderException(e.getMessage(), e);
            }
        }
        return connection;
    }
}
