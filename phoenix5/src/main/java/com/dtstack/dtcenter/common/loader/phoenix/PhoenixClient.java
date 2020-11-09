package com.dtstack.dtcenter.common.loader.phoenix;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.PhoenixSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.lang3.StringUtils;
import com.dtstack.dtcenter.loader.utils.DBUtil;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * 处理schema和表名
     *
     * @param schema
     * @param tableName
     * @return
     */
    protected String transferSchemaAndTableName(String schema,String tableName) {
        // schema为空直接返回
        if (StringUtils.isBlank(schema)) {
            return tableName;
        }
        if (!tableName.startsWith("\"") || !tableName.endsWith("\"")) {
            tableName = String.format("\"%s\"", tableName);
        }
        if (!schema.startsWith("\"") || !schema.endsWith("\"")){
            schema = String.format("\"%s\"", schema);
        }
        return String.format("%s.%s", schema, tableName);
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


    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(source, queryDTO, false);
        Phoenix5SourceDTO rdbmsSourceDTO = (Phoenix5SourceDTO) source;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            DatabaseMetaData meta = rdbmsSourceDTO.getConnection().getMetaData();
            if (null == queryDTO) {
                rs = meta.getTables(null, rdbmsSourceDTO.getSchema(), null, null);
            } else {
                rs = meta.getTables(null, rdbmsSourceDTO.getSchema(),
                        StringUtils.isBlank(queryDTO.getTableNamePattern()) ? queryDTO.getTableNamePattern() :
                                queryDTO.getTableName(),
                        DBUtil.getTableTypes(queryDTO));
            }
            while (rs.next()) {
                if (StringUtils.isBlank(rdbmsSourceDTO.getSchema()) && StringUtils.isNotBlank(rs.getString(2))) {
                    // 返回 schema.tableName形式 TODO 待FlinkX支持 "schema"."tableName"时修改
                    tableList.add(String.format("%s.%s", rs.getString(2), rs.getString(3)));
                }else {
                    tableList.add(rs.getString(3));
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException("获取数据库表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, null, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

}
