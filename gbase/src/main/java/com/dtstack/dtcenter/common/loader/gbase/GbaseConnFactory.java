package com.dtstack.dtcenter.common.loader.gbase;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.GBaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:58 2020/1/7
 * @Description：GBase8a 连接工厂
 */
public class GbaseConnFactory extends ConnFactory {
    public GbaseConnFactory() {
        this.driverName = DataBaseType.GBase8a.getDriverClassName();
        this.errorPattern = new GbaselErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO source, String taskParams) throws Exception {
        Connection conn = super.getConn(source, taskParams);
        GBaseSourceDTO gBaseSourceDTO = (GBaseSourceDTO) source;
        if (StringUtils.isBlank(gBaseSourceDTO.getSchema())) {
            return conn;
        }

        // 选择 Schema
        String useSchema = String.format("USE %s", gBaseSourceDTO.getSchema());
        DBUtil.executeSqlWithoutResultSet(conn, useSchema, false);
        return conn;
    }
}
