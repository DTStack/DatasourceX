package com.dtstack.dtcenter.common.loader.rdbms.db2;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:09 2020/1/7
 * @Description：Db2 工厂类
 */
@Slf4j
public class Db2ConnFactory extends ConnFactory {
    public Db2ConnFactory() {
        this.driverName = DataBaseType.DB2.getDriverClassName();
    }


    @Override
    public Boolean testConn(SourceDTO source) {
        boolean isConnected = false;
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConn(source);
            isConnected = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            DBUtil.closeDBResources(null, statement, conn);
        }
        return isConnected;
    }
}
