package com.dtstack.dtcenter.common.loader.rdbms.greenplum;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.sun.javafx.binding.StringFormatter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:13 2020/4/10
 * @Description：Greenplum 工厂
 */
public class GreenplumFactory extends ConnFactory {

    private static final String SCHEMA_SET="SET search_path TO %s";

    public GreenplumFactory() {
        driverName = DataBaseType.Greenplum6.getDriverClassName();
    }


    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        checkSchema(source);
        init();
        DriverManager.setLoginTimeout(30);
        Connection  connection = super.getConn(source);
        connection.createStatement().execute(String.format(SCHEMA_SET,source.getSchema()));
        return connection;
    }

    private void checkSchema(SourceDTO source){
        String schemaName = source.getSchema();
        if (StringUtils.isBlank(schemaName)) {
            throw new DtCenterDefException("greenplum6 数据源schema不能为空");
        }
    }

}
