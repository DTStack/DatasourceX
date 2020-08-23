package com.dtstack.dtcenter.common.loader.phoenix;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PhoenixSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:30 2020/7/8
 * @Description：Phoenix 客户端
 */
public class PhoenixClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new PhoenixConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Phoenix;
    }

    @Override
    protected String transferTableName(String tableName) {
        return tableName.contains("\"") ? tableName : String.format("\"%s\"", tableName);
    }

    @Override
    public String getTableMetaComment(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        PhoenixSourceDTO phoenixSourceDTO = (PhoenixSourceDTO) iSource;
        Integer clearStatus = beforeColumnQuery(phoenixSourceDTO, queryDTO);

        String tableName = queryDTO.getTableName();
        if (tableName.contains(".")) {
            tableName = tableName.split("\\.")[1];
        }
        tableName = tableName.replace("\"", "");

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = phoenixSourceDTO.getConnection().getMetaData();
            resultSet = metaData.getTables(null, null, tableName, null);
            while (resultSet.next()) {
                String comment = resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                return comment;
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()), e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, phoenixSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return "";
    }
}
