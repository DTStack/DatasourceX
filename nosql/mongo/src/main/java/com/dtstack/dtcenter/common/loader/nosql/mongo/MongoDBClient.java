package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.loader.nosql.common.AbsNosqlClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:24 2020/2/5
 * @Description：MongoDB 客户端
 */
public class MongoDBClient extends AbsNosqlClient {
    @Override
    public Boolean testCon(SourceDTO source) throws Exception {
        return MongoDBUtils.checkConnection(source);
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, String sql) throws Exception {
        return null;
    }
}