package com.dtstack.dtcenter.common.loader.dm;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DmSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:19 2020/4/17
 * @Description：达梦客户端
 */
public class DmClient extends AbsRdbmsClient {
    private static final String ORACLE_ALL_TABLES_SQL = "SELECT TABLE_NAME FROM USER_TABLES UNION SELECT GRANTOR||'" +
            ".'||'\"'||TABLE_NAME||'\"' FROM ALL_TAB_PRIVS WHERE grantee = (SELECT USERNAME FROM user_users WHERE " +
            "ROWNUM = 1) ";
    private static final String ORACLE_WITH_VIEWS_SQL = "UNION SELECT VIEW_NAME FROM USER_VIEWS ";

    private static  String DM_ALL_DATABASES = "SELECT DISTINCT OWNER FROM SYS.DBA_TABLES WHERE TABLESPACE_NAME = 'MAIN'";

    private static String CREATE_TABLE_SQL = "select dbms_metadata.get_ddl(OBJECT_TYPE => 'TABLE',\n" +
            "NAME=>upper('%s'),SCHNAME => '%s')";

    // 获取当前版本号
    private static final String SHOW_VERSION = "select BANNER from v$version";

    @Override
    protected ConnFactory getConnFactory() {
        return new DmConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DMDB;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);

        DmSourceDTO dmSourceDTO = (DmSourceDTO) iSource;
        Statement statement = null;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            String sql = queryDTO != null && queryDTO.getView() ? ORACLE_ALL_TABLES_SQL + ORACLE_WITH_VIEWS_SQL :
                    ORACLE_ALL_TABLES_SQL;
            statement = dmSourceDTO.getConnection().createStatement();
            rs = statement.executeQuery(sql);
            int columnSize = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("get table exception,%s", e.getMessage()), e);
        } finally {
            DBUtil.closeDBResources(rs, statement, DBUtil.clearAfterGetConnection(dmSourceDTO, clearStatus));
        }
        return tableList;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        DmSourceDTO dmSourceDTO = (DmSourceDTO) source;
        DmDownloader dmDownloader = new DmDownloader(getCon(dmSourceDTO), queryDTO.getSql(), dmSourceDTO.getSchema());
        dmDownloader.configure();
        return dmDownloader;
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        DmSourceDTO dmSourceDTO = (DmSourceDTO) source;
        queryDTO.setSql(String.format(CREATE_TABLE_SQL,queryDTO.getTableName(),dmSourceDTO.getSchema()));
        return super.getCreateTableSql(source,queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getShowDbSql() {
        return DM_ALL_DATABASES;
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }
}
