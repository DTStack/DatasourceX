package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.loader.nosql.common.AbsNosqlClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:24 2020/2/5
 * @Description：MongoDB 客户端
 */
@Slf4j
public class MongoDBClient extends AbsNosqlClient {
    @Override
    public Boolean testCon(SourceDTO source) {
        return MongoDBUtils.checkConnection(source);
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return MongoDBUtils.getTableList(source);
    }

}