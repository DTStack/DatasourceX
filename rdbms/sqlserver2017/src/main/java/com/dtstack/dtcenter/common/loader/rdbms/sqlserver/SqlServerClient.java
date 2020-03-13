package com.dtstack.dtcenter.common.loader.rdbms.sqlserver;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：SqlServer 客户端
 */
public class SqlServerClient extends AbsRdbmsClient {
    private static final String TABLE_QUERY_ALL = "select * from sys.objects where type='U' or type='V'";
    private static final String TABLE_QUERY = "select * from sys.objects where type='U'";

    @Override
    protected ConnFactory getConnFactory() {
        return new SQLServerConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.SQLSERVER_2017_LATER;
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, false);

        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO.getView() ? TABLE_QUERY_ALL : TABLE_QUERY;
            statement = source.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, source.clearAfterGetConnection(closeQuery));
        }
        return tableList;
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeColumnQuery(source, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = source.getConnection().createStatement();
            resultSet = statement.executeQuery(
                    "select c.name, cast(isnull(f.[value], '') as nvarchar(100)) as REMARKS\n" +
                            "from sys.objects c " +
                            "left join sys.extended_properties f on f.major_id = c.object_id and f.minor_id = 0 and f.class = 1\n" +
                            "where c.type = 'u'");
            while (resultSet.next()) {
                String dbTableName = resultSet.getString(1);
                if (dbTableName.equalsIgnoreCase(queryDTO.getTableName())) {
                    return resultSet.getString(DtClassConsistent.PublicConsistent.REMARKS);
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(closeQuery));
        }
        return null;
    }
}
