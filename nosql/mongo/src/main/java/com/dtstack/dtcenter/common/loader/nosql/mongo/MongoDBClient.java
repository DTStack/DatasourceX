package com.dtstack.dtcenter.common.loader.nosql.mongo;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.nosql.common.AbsNosqlClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

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
        if (queryDTO == null || StringUtils.isBlank(source.getSchema())) {
            throw new DtCenterDefException("数据源 DB 不能为空");
        }

        List<String> tableList = Lists.newArrayList();
        List<ServerAddress> serverAddress = MongoDBUtils.getServerAddress(source.getHostPort());
        MongoClient mongoClient = null;
        try {
            if (StringUtils.isEmpty(source.getUsername()) || StringUtils.isEmpty(source.getPassword())) {
                mongoClient = new MongoClient(serverAddress);
            } else {
                MongoCredential credential = MongoCredential.createScramSha1Credential(source.getUsername(),
                        source.getSchema(),
                        source.getPassword() == null ? null : source.getPassword().toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);

                mongoClient = new MongoClient(serverAddress, credentials);
            }

            MongoDatabase mongoDatabase = mongoClient.getDatabase(source.getSchema());
            MongoIterable<String> tableNames = mongoDatabase.listCollectionNames();
            for (String s : tableNames) {
                tableList.add(s);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        return tableList;
    }
}