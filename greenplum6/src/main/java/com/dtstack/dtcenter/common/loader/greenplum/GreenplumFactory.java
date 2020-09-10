package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.loader.common.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.Greenplum6SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:13 2020/4/10
 * @Description：Greenplum 工厂
 */
public class GreenplumFactory extends ConnFactory {

    private static final String SCHEMA_SET = "SET search_path TO %s";

    public GreenplumFactory() {
        driverName = DataBaseType.Greenplum6.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        Greenplum6SourceDTO greenplum6SourceDTO = (Greenplum6SourceDTO) iSource;
        DriverManager.setLoginTimeout(30);
        Connection connection = super.getConn(greenplum6SourceDTO);
        if (!StringUtils.isBlank(greenplum6SourceDTO.getSchema())) {
            DBUtil.executeSqlWithoutResultSet(connection, String.format(SCHEMA_SET, greenplum6SourceDTO.getSchema()), false);
        }
        return connection;
    }
}
