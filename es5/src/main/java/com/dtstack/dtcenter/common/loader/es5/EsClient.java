package com.dtstack.dtcenter.common.loader.es5;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.es5.pool.ElasticSearchManager;
import com.dtstack.dtcenter.common.loader.es5.pool.ElasticSearchPool;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * es5 连接客户端
 *
 * @author ：wangchuan
 * date：Created in 下午3:04 2021/12/8
 * company: www.dtstack.com
 */
@Slf4j
public class EsClient<T> extends AbsNoSqlClient<T> {

    private static final int MAX_NUM = 10000;

    private static final ElasticSearchManager ELASTIC_SEARCH_MANAGER = ElasticSearchManager.getInstance();

    public static final ThreadLocal<Boolean> IS_OPEN_POOL = new ThreadLocal<>();

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return false;
        }
        TransportClient client = null;
        try {
            client = getClient(esSourceDTO);
            client.listedNodes();
            return true;
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(client, esSourceDTO);
        }
    }

    /**
     * 获取 es 某一索引下所有type
     *
     * @param sourceDTO 数据源信息
     * @param queryDTO  查询信息
     * @return type 集合
     */
    @Override
    public List<String> getTableList(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) sourceDTO;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        TransportClient client = getClient(esSourceDTO);
        List<String> typeList = Lists.newArrayList();
        // es索引
        String index = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : queryDTO.getTableName();
        // 不指定index抛异常
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified, and the acquisition fails");
        }
        try {
            GetMappingsRequest request = new GetMappingsRequest();
            request.indicesOptions(null);
            GetMappingsResponse response = client.admin().indices().getMappings(request).get();
            ImmutableOpenMap<String, MappingMetaData> typeMappings = response.mappings().get(index);
            for (ObjectCursor<String> typeName : typeMappings.keys()) {
                typeList.add(typeName.value);
            }
        } catch (NullPointerException e) {
            throw new DtLoaderException(String.format("index not exits,%s", e.getMessage()), e);
        } catch (Exception e) {
            log.error(String.format("get type exception,%s", e.getMessage()), e);
        } finally {
            closeResource(client, esSourceDTO);
        }
        return typeList;
    }

    /**
     * 获取 es 所有索引
     *
     * @param sourceDTO 数据源连接信息
     * @param queryDTO  查询条件
     * @return 所有 index
     */
    @Override
    public List<String> getAllDatabases(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) sourceDTO;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        TransportClient client = getClient(esSourceDTO);
        ArrayList<String> dbs = new ArrayList<>();
        try {
            GetAliasesRequest aliasesRequest = new GetAliasesRequest();
            GetAliasesResponse aliasesResponse = client.admin().indices().getAliases(aliasesRequest).get();
            ImmutableOpenMap<String, List<AliasMetaData>> aliases = aliasesResponse.getAliases();
            for (ObjectCursor<String> key : aliases.keys()) {
                dbs.add(key.value);
            }
        } catch (Exception e) {
            log.error(String.format("Failed to get es index,%s", e.getMessage()), e);
        } finally {
            closeResource(client, esSourceDTO);
        }
        return dbs;
    }

    /**
     * es 数据预览，默认100条，最大10000条
     *
     * @param sourceDTO 数据源连接信息
     * @param queryDTO  查询条件
     * @return 预览的数据
     */
    @Override
    public List<List<Object>> getPreview(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) sourceDTO;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        TransportClient client = getClient(esSourceDTO);
        //索引
        String index = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : queryDTO.getTableName();
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified，Data preview failed");
        }
        //限制条数，最大10000条
        int previewNum = queryDTO.getPreviewNum() > MAX_NUM ? MAX_NUM : queryDTO.getPreviewNum();
        //根据index进行查询
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder query = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(previewNum);
        SearchRequest source = searchRequest.source(query);
        List<List<Object>> documentList = Lists.newArrayList();
        try {
            SearchResponse search = client.search(source).get();
            //结果集
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                //一行数据
                List<Object> document = Lists.newArrayList();
                Map<String, Object> sourceAsMap = hit.getSource();
                sourceAsMap.keySet().forEach(key ->
                        document.add(new Pair<String, Object>(key, sourceAsMap.get(key))));
                documentList.add(document);
            }
        } catch (Exception e) {
            log.error("doc acquisition exception", e);
        } finally {
            closeResource(client, esSourceDTO);
        }
        return documentList;
    }

    /**
     * 获取 es 字段信息
     *
     * @param sourceDTO 数据源连接信息
     * @param queryDTO  查询条件
     * @return es 字段集合
     */
    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) sourceDTO;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        TransportClient client = getClient(esSourceDTO);
        //索引
        String index = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : queryDTO.getTableName();
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified, and the field information fails to be obtained");
        }
        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        try {
            //根据index进行查询
            GetMappingsRequest request = new GetMappingsRequest();
            request.indicesOptions(null);
            GetMappingsResponse response = client.admin().indices().getMappings(request).get();
            ImmutableOpenMap<String, MappingMetaData> cursors = response.getMappings().get(index);

            String type = StringUtils.isNotBlank(queryDTO.getTableName()) ? queryDTO.getTableName() : "_doc";
            MappingMetaData mappingMetaData = cursors.get(type);
            if (Objects.isNull(mappingMetaData)) {
                return columnMetaDTOS;
            }
            Map<String, Object> metaDataMap = (Map<String, Object>) mappingMetaData.getSourceAsMap().get("properties");
            Set<String> metaData = metaDataMap.keySet();
            for (String meta : metaData) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(meta);
                Map<String, Object> map = (Map<String, Object>) metaDataMap.get(meta);
                String columnType = (String) map.get("type");
                columnMetaDTO.setType(columnType);
                columnMetaDTOS.add(columnMetaDTO);
            }
        } catch (Exception e) {
            log.error("doc acquisition exception", e);
        } finally {
            closeResource(client, esSourceDTO);
        }
        return columnMetaDTOS;
    }

    private static TransportClient getClient(ESSourceDTO esSourceDTO) {
        boolean check = esSourceDTO.getPoolConfig() != null;
        IS_OPEN_POOL.set(check);
        if (!check) {
            return getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        }
        ElasticSearchPool elasticSearchPool = ELASTIC_SEARCH_MANAGER.getConnection(esSourceDTO);
        TransportClient restHighLevelClient = elasticSearchPool.getResource();
        if (Objects.isNull(restHighLevelClient)) {
            throw new DtLoaderException("No database connection available");
        }
        return restHighLevelClient;

    }

    /**
     * 根据 es 地址获取客户端
     *
     * @param address  es 连接地址
     * @param username 用户名
     * @param password 密码
     * @return es client
     */
    private static TransportClient getClient(String address, String username, String password) {
        log.info("Get ES data source connection, address : {}, userName : {}", address, username);
        List<HttpHost> httpHosts = dealHost(address);
        TransportAddress[] transportAddresses = httpHosts
                .stream()
                .map(host -> {
                    try {
                        return new InetSocketTransportAddress(InetAddress.getByName(host.getHostName()), host.getPort());
                    } catch (UnknownHostException e) {
                        log.error(e.getMessage(), e);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .toArray(TransportAddress[]::new);
        Settings settings = Settings.builder()
                .put("client.transport.ignore_cluster_name", true)
                .put("client.transport.sniff", false)
                .put("client.transport.ping_timeout", "30s")
                .build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddresses(transportAddresses);
    }

    private static List<HttpHost> dealHost(String address) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }
        return httpHostList;
    }

    private void closeResource(TransportClient transportClient, ESSourceDTO esSourceDTO) {
        if (BooleanUtils.isFalse(IS_OPEN_POOL.get())) {
            //未开启线程池
            try {
                if (Objects.nonNull(transportClient)) {
                    transportClient.close();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            IS_OPEN_POOL.remove();
        } else {
            //开启连接池
            ElasticSearchPool elasticSearchPool = ELASTIC_SEARCH_MANAGER.getConnection(esSourceDTO);
            try {
                if (Objects.nonNull(transportClient) && Objects.nonNull(elasticSearchPool)) {
                    elasticSearchPool.returnResource(transportClient);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
