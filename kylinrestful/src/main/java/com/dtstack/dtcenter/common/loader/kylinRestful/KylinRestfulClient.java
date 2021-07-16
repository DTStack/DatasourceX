package com.dtstack.dtcenter.common.loader.kylinRestful;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.request.RestfulClientFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;

import java.util.List;

/**
 * kylin 客户端
 *
 * @author ：qianyi
 * date：Created in 上午10:33 2021/7/14
 * company: www.dtstack.com
 */
public class KylinRestfulClient<T> extends AbsNoSqlClient<T> {


    @Override
    public Boolean testCon(ISourceDTO source) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.auth(kylinRestfulSourceDTO);
    }


    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveDbList(kylinRestfulSourceDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getAllHiveTableListBySchema(kylinRestfulSourceDTO, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        KylinRestfulSourceDTO kylinRestfulSourceDTO = (KylinRestfulSourceDTO) source;
        RestfulClient restfulClient = RestfulClientFactory.getRestfulClient();
        return restfulClient.getHiveColumnMetaData(kylinRestfulSourceDTO, queryDTO);
    }

}
