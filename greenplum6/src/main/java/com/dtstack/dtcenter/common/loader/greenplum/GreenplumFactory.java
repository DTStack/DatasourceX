package com.dtstack.dtcenter.common.loader.greenplum;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
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
//        checkSchema(source);
        init();
        DriverManager.setLoginTimeout(30);
        Connection  connection = super.getConn(source);
        if(!StringUtils.isBlank(source.getSchema())){
            connection.createStatement().execute(String.format(SCHEMA_SET,source.getSchema()));
        }
        return connection;
    }



}
