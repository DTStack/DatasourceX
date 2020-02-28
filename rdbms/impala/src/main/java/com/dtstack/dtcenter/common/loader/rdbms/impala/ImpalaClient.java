package com.dtstack.dtcenter.common.loader.rdbms.impala;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 连接
 */
public class ImpalaClient extends AbsRdbmsClient {
    private static Pattern IMPALA_JDBC_PATTERN = Pattern.compile("(?i)jdbc:impala://[0-9a-zA-Z\\-\\.]+:[\\d]+/" +
            "(?<db>[0-9a-zA-Z\\-]+);.*");

    @Override
    protected ConnFactory getConnFactory() {
        return new ImpalaCoonFactory();
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        Boolean closeQuery = beforeQuery(source, queryDTO, false);
        //impala db写在jdbc连接中无效，必须手动切换库
        String db = StringUtils.isBlank(queryDTO.getSchema()) ?
                getImpalaDbFromJdbc(source.getUrl(), DataSourceType.IMPALA) : queryDTO.getSchema();
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
            DBUtil.closeDBResources(rs, statement, closeQuery ? source.getConnection() : null);
        }
        return tableList;
    }

    private static String getImpalaDbFromJdbc(String jdbcUrl, DataSourceType sourceType) {
        if (!DataSourceType.IMPALA.equals(sourceType)) {
            return null;
        }
        if (StringUtils.isEmpty(jdbcUrl)) {
            return null;
        }
        Matcher matcher = IMPALA_JDBC_PATTERN.matcher(jdbcUrl);
        String db = "";
        if (matcher.matches()) {
            db = matcher.group(1);
        }
        return db;
    }
}
