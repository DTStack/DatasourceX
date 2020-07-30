package com.dtstack.dtcenter.common.loader.libra;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.LibraSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:54 2020/2/29
 * @Description：Libra 客户端
 */
public class LibraClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new LibraConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.LIBRA;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        LibraSourceDTO libraSourceDTO = (LibraSourceDTO) iSource;
        Integer clearStatus = beforeQuery(libraSourceDTO, queryDTO, false);
        if (queryDTO == null || StringUtils.isBlank(libraSourceDTO.getSchema())) {
            return super.getTableList(libraSourceDTO, queryDTO);
        }

        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = libraSourceDTO.getConnection().createStatement();
            //大小写区分
            rs = statement.executeQuery(String.format("select table_name from information_schema.tables WHERE " +
                    "table_schema in ( '%s' )", libraSourceDTO.getSchema()));
            List<String> tableList = new ArrayList<>();
            while (rs.next()) {
                tableList.add(rs.getString(1));
            }
            return tableList;
        } catch (Exception e) {
            throw new DtCenterDefException("获取表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, statement, libraSourceDTO.clearAfterGetConnection(clearStatus));
        }
    }
}
