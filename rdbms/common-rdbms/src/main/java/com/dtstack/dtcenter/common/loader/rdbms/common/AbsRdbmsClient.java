package com.dtstack.dtcenter.common.loader.rdbms.common;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/1/3
 * @Description：客户端
 */
public abstract class AbsRdbmsClient implements IClient {
    private static final Logger logger = LoggerFactory.getLogger(ConnFactory.class);

    private ConnFactory connFactory;

    protected String dbType = "rdbms";

    protected abstract ConnFactory getConnFactory();

    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        logger.warn("-------get {} connection success-----", dbType);

        connFactory = getConnFactory();
        connFactory.init(source.getUrl(), source.getProperties());
        return connFactory.getConn();
    }

    @Override
    public Boolean testCon(SourceDTO source) throws ClassNotFoundException {
        connFactory = getConnFactory();
        connFactory.init(source.getUrl(), source.getProperties());
        return connFactory.testConn();
    }

    @Override
    public List<Map<String, Object>> executeQuery(Connection connection, String sql) throws Exception {
        if (connection == null || connection.isClosed()) {
            return Lists.newArrayList();
        }

        List<Map<String, Object>> result = Lists.newArrayList();
        ResultSet res = null;
        Statement statement = null;
        try {
            statement = connection.createStatement();
            res = statement.executeQuery(sql);
            int columns = res.getMetaData().getColumnCount();
            List<String> columnName = Lists.newArrayList();
            for (int i = 0; i < columns; i++) {
                columnName.add(res.getMetaData().getColumnName(i + 1));
            }

            while (res.next()) {
                Map<String, Object> row = Maps.newLinkedHashMap();
                ;
                for (int i = 0; i < columns; i++) {
                    row.put(columnName.get(i), res.getObject(i + 1));
                }
                result.add(row);
            }
        } catch (Exception e) {
            throw new DtCenterDefException(DBErrorCode.SQL_EXE_EXCEPTION, e);
        } finally {
            if (res != null) {
                res.close();
            }
            DBUtil.closeDBResources(res, statement, null);
        }
        return result;
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, String sql) throws Exception {
        return executeQuery(getCon(source), sql);
    }
}
