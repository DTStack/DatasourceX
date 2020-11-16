package com.dtstack.dtcenter.common.loader.clickhouse;

import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ClickHouseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:57 2020/1/7
 * @Description：clickhouse 客户端
 */
public class ClickhouseClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new ClickhouseConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Clickhouse;
    }

    private static String PARTITION_COLUMN_SQL = "select name,type,comment from system.columns where database = '%s' and table = '%s' and is_in_partition_key = 1";

    private static final String DONT_EXIST = "doesn't exist";

    // 获取正在使用数据库
    private static final String CURRENT_DB = "select currentDatabase()";

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        ClickHouseSourceDTO clickHouseSourceDTO = (ClickHouseSourceDTO) iSource;

        // 获取表信息需要通过show tables 语句
        String sql = "show tables";
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = clickHouseSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, clickHouseSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        ClickHouseSourceDTO clickHouseSourceDTO = (ClickHouseSourceDTO) source;
        String sql = String.format(PARTITION_COLUMN_SQL,clickHouseSourceDTO.getSchema(),queryDTO.getTableName());
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columnList = new ArrayList<>();
        try {
            statement = clickHouseSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rs.getString(1));
                columnMetaDTO.setType(rs.getString(2));
                columnMetaDTO.setComment(rs.getString(3));
                columnMetaDTO.setPart(true);
                columnList.add(columnMetaDTO);
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, clickHouseSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return columnList;
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);
        ClickHouseSourceDTO postgresqlSourceDTO = (ClickHouseSourceDTO) source;
        Statement statement = null;
        ResultSet rs = null;
        List<ColumnMetaDTO> columns = new ArrayList<>();
        try {
            statement = postgresqlSourceDTO.getConnection().createStatement();
            String queryColumnSql = "select * from " + queryDTO.getTableName()
                    + " where 1=2";
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(rsMetaData.getColumnName(i + 1));
                String type = rsMetaData.getColumnTypeName(i + 1);
                int columnType = rsMetaData.getColumnType(i + 1);
                int precision = rsMetaData.getPrecision(i + 1);
                int scale = rsMetaData.getScale(i + 1);
                //clickhouse类型转换
                String flinkSqlType = ClickhouseAdapter.mapColumnTypeJdbc2Java(columnType, precision, scale);
                if (StringUtils.isNotEmpty(flinkSqlType)) {
                    type = flinkSqlType;
                }
                columnMetaDTO.setType(type);
                // 获取字段精度
                if (columnMetaDTO.getType().equalsIgnoreCase("decimal")
                        || columnMetaDTO.getType().equalsIgnoreCase("float")
                        || columnMetaDTO.getType().equalsIgnoreCase("double")
                        || columnMetaDTO.getType().equalsIgnoreCase("numeric")) {
                    columnMetaDTO.setScale(rsMetaData.getScale(i + 1));
                    columnMetaDTO.setPrecision(rsMetaData.getPrecision(i + 1));
                }
                columns.add(columnMetaDTO);
            }
            return columns;

        } catch (SQLException e) {
            if (e.getMessage().contains(DONT_EXIST)) {
                throw new DtCenterDefException(queryDTO.getTableName() + "表不存在", DBErrorCode.TABLE_NOT_EXISTS, e);
            } else {
                throw new DtCenterDefException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", queryDTO.getTableName()),
                        DBErrorCode.GET_COLUMN_INFO_FAILED, e);
            }
        } finally {
            DBUtil.closeDBResources(rs, statement, postgresqlSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        ClickHouseSourceDTO clickHouseSourceDTO = (ClickHouseSourceDTO) source;
        ClickHouseDownloader clickHouseDownloader = new ClickHouseDownloader(getCon(clickHouseSourceDTO), queryDTO.getSql(), clickHouseSourceDTO.getSchema());
        clickHouseDownloader.configure();
        return clickHouseDownloader;
    }

    @Override
    protected String getCurrentDbSql() {
        return CURRENT_DB;
    }
}
