package com.dtstack.dtcenter.common.loader.rdbms.impala;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DBErrorCode;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 连接
 */
public class ImpalaClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new ImpalaCoonFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.IMPALA;
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        //impala db写在jdbc连接中无效，必须手动切换库
        String db = queryDTO == null || StringUtils.isBlank(source.getSchema()) ?
                getImpalaDbFromJdbc(source.getUrl()) : source.getSchema();
        // 获取表信息需要通过show tables 语句
        String sql = "show tables";
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            statement = source.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(columnSize == 1 ? 1 : 2));
            }
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, source.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);

        List<ColumnMetaDTO> columnList = new ArrayList<>();
        Statement stmt = null;
        ResultSet resultSet = null;
        try {
            LinkedHashMap<String, JSONObject> colNameMap = new LinkedHashMap<>();
            stmt = source.getConnection().createStatement();
            //首先判断是否是kudu表 是kudu表直接用主键代替 isPart
            resultSet = stmt.executeQuery("DESCRIBE " + queryDTO.getTableName());
            int columnCnt = resultSet.getMetaData().getColumnCount();

            // kudu表
            if (columnCnt > 3) {
                while (resultSet.next()) {
                    columnList.add(dealResult(resultSet,
                            resultSet.getString(DtClassConsistent.PublicConsistent.PRIMARY_KEY)));
                }
                return columnList;
            }

            //hive表 继续获取分区字段 先关闭之前的 rs
            resultSet.close();
            resultSet = stmt.executeQuery("DESCRIBE formatted " + queryDTO.getTableName());
            while (resultSet.next()) {
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.NAME).trim();

                if (StringUtils.isEmpty(colName)) {
                    continue;
                }
                if (colName.startsWith("#") && colName.contains(DtClassConsistent.PublicConsistent.COL_NAME)) {
                    continue;

                }
                if (colName.startsWith("#") || colName.contains("Partition Information")) {
                    break;
                }

                if (StringUtils.isNotBlank(colName)) {
                    columnList.add(dealResult(resultSet, Boolean.FALSE));
                }
            }

            while (resultSet.next() && !queryDTO.getFilterPartitionColumns()) {
                String colName = resultSet.getString(DtClassConsistent.PublicConsistent.NAME);
                if (StringUtils.isBlank(colName)) {
                    continue;
                }
                if (colName.startsWith("#") && colName.contains(DtClassConsistent.PublicConsistent.COL_NAME)) {
                    continue;
                }
                if (colName.contains("Detailed Table Information") || colName.contains("Database:")) {
                    break;
                }

                columnList.add(dealResult(resultSet, Boolean.TRUE));
            }

        } catch (SQLException e) {
            throw new DtCenterDefException(String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, stmt, null);
        }

        return columnList;
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeColumnQuery(source, queryDTO);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            statement = source.getConnection().createStatement();
            if (StringUtils.isNotEmpty(source.getSchema())) {
                statement.execute(String.format(DtClassConsistent.PublicConsistent.USE_DB, source.getSchema()));
            }
            resultSet = statement.executeQuery(String.format(DtClassConsistent.HadoopConfConsistent.DESCRIBE_EXTENDED
                    , queryDTO.getTableName()));
            while (resultSet.next()) {
                String columnType = resultSet.getString(2);
                if (StringUtils.isNotBlank(columnType) && columnType.contains("comment")) {
                    return StringUtils.isBlank(resultSet.getString(3)) ? "" : resultSet.getString(3).trim();
                }
            }
        } catch (Exception e) {
            throw new DtCenterDefException(String.format("获取表:%s 的信息时失败. 请联系 DBA 核查该库、表信息.",
                    queryDTO.getTableName()),
                    DBErrorCode.GET_COLUMN_INFO_FAILED, e);
        } finally {
            DBUtil.closeDBResources(resultSet, statement, source.clearAfterGetConnection(clearStatus));
        }
        return "";
    }

    private static ColumnMetaDTO dealResult(ResultSet resultSet, Object part) throws SQLException {
        ColumnMetaDTO metaDTO = new ColumnMetaDTO();
        metaDTO.setKey(resultSet.getString(DtClassConsistent.PublicConsistent.NAME).trim());
        metaDTO.setType(resultSet.getString(DtClassConsistent.PublicConsistent.TYPE).trim());
        metaDTO.setComment(resultSet.getString(DtClassConsistent.PublicConsistent.COMMENT));
        metaDTO.setPart(Boolean.TRUE.equals(part));
        return metaDTO;
    }

    private static String getImpalaDbFromJdbc(String jdbcUrl) {
        if (StringUtils.isEmpty(jdbcUrl)) {
            return null;
        }
        Matcher matcher = DtClassConsistent.PatternConsistent.IMPALA_JDBC_PATTERN.matcher(jdbcUrl);
        String db = "";
        if (matcher.matches()) {
            db = matcher.group(1);
        }
        return db;
    }
}
