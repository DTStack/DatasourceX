package com.dtstack.dtcenter.common.loader.kylin;

import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:14 2020/1/7
 * @Description：Kylin 客户端
 */
public class KylinClient extends AbsRdbmsClient {
    private static final String TABLE_SHOW = "\"%s\".\"%s\"";

    @Override
    protected ConnFactory getConnFactory() {
        return new KylinConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Kylin;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        KylinSourceDTO kylinSourceDTO = (KylinSourceDTO) source;
        KylinDownloader kylinDownloader = new KylinDownloader(getCon(kylinSourceDTO), queryDTO.getSql(), kylinSourceDTO.getSchema());
        kylinDownloader.configure();
        return kylinDownloader;
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        Integer clearStatus = beforeQuery(iSource, queryDTO, false);
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
        ResultSet rs = null;
        List<String> tableList = new ArrayList<>();
        try {
            DatabaseMetaData meta = rdbmsSourceDTO.getConnection().getMetaData();
            if (null == queryDTO) {
                rs = meta.getTables(null, null, null, null);
            } else {
                rs = meta.getTables(null, rdbmsSourceDTO.getSchema(),
                        StringUtils.isBlank(queryDTO.getTableNamePattern()) ? queryDTO.getTableNamePattern() :
                                queryDTO.getTableName(),
                        DBUtil.getTableTypes(queryDTO));
            }
            while (rs.next()) {
                tableList.add(String.format(TABLE_SHOW, rs.getString(2), rs.getString(3)));
            }
        } catch (Exception e) {
            throw new DtLoaderException("获取数据库表异常", e);
        } finally {
            DBUtil.closeDBResources(rs, null, rdbmsSourceDTO.clearAfterGetConnection(clearStatus));
        }
        return tableList;
    }

}
