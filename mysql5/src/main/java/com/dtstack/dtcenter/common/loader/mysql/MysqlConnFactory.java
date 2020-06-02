package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Mysql5SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:11 2020/1/3
 * @Description：Mysql 连接
 */
public class MysqlConnFactory extends ConnFactory {
    public MysqlConnFactory() {
        driverName = DataBaseType.MySql.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO source) throws Exception {
        if (source == null) {
            throw new DtCenterDefException("数据源信息为 NULL");
        }

        Mysql5SourceDTO mysql5SourceDTO = (Mysql5SourceDTO) source;

        init();
        DriverManager.setLoginTimeout(5);
        String url = dealSourceUrl(mysql5SourceDTO);
        if (StringUtils.isBlank(mysql5SourceDTO.getUsername())) {
            return DriverManager.getConnection(mysql5SourceDTO.getUrl());
        }

        return DriverManager.getConnection(url, mysql5SourceDTO.getUsername(), mysql5SourceDTO.getPassword());
    }

    protected String dealSourceUrl(RdbmsSourceDTO rdbmsSourceDTO) {
        String schema = rdbmsSourceDTO.getSchema();
        String url = rdbmsSourceDTO.getUrl();
        if (StringUtils.isNotEmpty(schema)){
            String[] urlAyy = url.split("/");
            if (urlAyy.length > 2){
                url = urlAyy[0] + "//" + urlAyy[2] + "/" +schema;
            }
        }
        return url;
    }
}
