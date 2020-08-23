package com.dtstack.dtcenter.common.loader.kylin;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:14 2020/1/7
 * @Description：Kylin 客户端
 */
public class KylinClient extends AbsRdbmsClient {
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
}
