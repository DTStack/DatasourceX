/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.es.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.PathUtils;
import com.dtstack.dtcenter.common.loader.common.utils.SSLUtil;
import com.dtstack.dtcenter.common.loader.common.utils.SftpUtil;
import com.dtstack.dtcenter.common.loader.common.utils.TelUtil;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.AliasEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.ColumnEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.IndexProperties;
import com.dtstack.dtcenter.common.loader.es.metadata.entity.MetaDataEsEntity;
import com.dtstack.dtcenter.common.loader.es.metadata.util.CommonUtils;
import com.dtstack.dtcenter.common.loader.es.metadata.util.EsUtil;
import com.dtstack.dtcenter.common.loader.es.pool.ElasticSearchManager;
import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.dtstack.dtcenter.common.loader.es.metadata.cons.EsClientCons.KEY_FILENAME;
import static com.dtstack.dtcenter.common.loader.es.metadata.cons.EsClientCons.KEY_HTTPS;
import static com.dtstack.dtcenter.common.loader.es.metadata.cons.EsClientCons.KEY_KEYSTORE_PASS;
import static com.dtstack.dtcenter.common.loader.es.metadata.cons.EsClientCons.KEY_PKCS12;
import static com.dtstack.dtcenter.common.loader.es.metadata.cons.EsClientCons.KEY_TYPE;

/**
 * es元数据收集器
 *
 * @author luming
 * @date 2022/4/12
 */
public class EsMetadataCollect extends AbstractMetaDataCollect {

    private ESSourceDTO esSource;

    private RestClient lowClient;

    @Override
    protected void openConnection() {
        esSource = (ESSourceDTO) sourceDTO;
        if (esSource == null || StringUtils.isBlank(esSource.getUrl())) {
            throw new IllegalArgumentException("sourceDTO has error, please check");
        }

        lowClient = getLowClient();

        if (CollectionUtils.isEmpty(tableList)) {
            tableList = showTables();
        }
    }

    /**
     * 获取es low level client
     *
     * @return client
     */
    private RestClient getLowClient() {
        if (Objects.nonNull(esSource.getSslConfig())
                && MapUtils.isNotEmpty(esSource.getSslConfig().getOtherConfig())) {
            return getLowClientWithSsl();
        } else {
            RestHighLevelClient client = getClient();
            return client.getLowLevelClient();
        }
    }

    /**
     * 获取带有ssl认证的es low level client
     *
     * @return client
     */
    private RestClient getLowClientWithSsl() {
        List<HttpHost> hostList = splitAddress(esSource.getUrl());

        Map<String, Object> otherConfig = esSource.getSslConfig().getOtherConfig();
        String type = MapUtils.getString(otherConfig, KEY_TYPE, KEY_PKCS12);
        String keyStorePass = MapUtils.getString(otherConfig, KEY_KEYSTORE_PASS, "");
        String fileName = MapUtils.getString(otherConfig, KEY_FILENAME);
        String remoteDir = esSource.getSslConfig().getRemoteSSLDir();

        // get certificate filepath
        String localConfDir = SftpUtil.downloadSftpDirFromSftp(esSource, remoteDir, PathUtils.getConfDir());
        Path trustStorePath = Paths.get(localConfDir);
        // use the certificate to obtain KeyStore
        KeyStore trustStore = SSLUtil.getKeyStoreByType(type, trustStorePath, keyStorePass);
        try {
            SSLContext sslContext = SSLContexts
                    .custom()
                    .loadTrustMaterial(trustStore, (x509Certificates, s) -> true)
                    .build();

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(esSource.getUsername(), esSource.getPassword()));

            RestClientBuilder restClientBuilder = RestClient
                    .builder(hostList.toArray(new HttpHost[0]))
                    .setHttpClientConfigCallback(
                            httpClientBuilder -> {
                                httpClientBuilder.setSSLContext(sslContext);
                                httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                return httpClientBuilder;
                            });

//            Integer timeout = esSource.get
//            if (timeout != null) {
//                restClientBuilder.setMaxRetryTimeoutMillis(timeout * 1000);
//            }

//            String pathPrefix = clientConfig.getPathPrefix();
//            if (StringUtils.isNotEmpty(pathPrefix)) {
//                restClientBuilder.setPathPrefix(pathPrefix);
//            }
            return restClientBuilder.build();
        } catch (Exception e) {
            throw new DtLoaderException("es get client with ssl error:" + e.getMessage(), e);
        }
    }

    /**
     * 获取es high level client
     *
     * @return client
     */
    private RestHighLevelClient getClient() {
        if (esSource.getPoolConfig() == null) {
            String username = esSource.getUsername();
            String password = esSource.getPassword();
            HttpHost[] httpHosts = CommonUtils.list2Arr(CommonUtils.dealHost(esSource.getUrl()));

            //有用户名密码情况
            if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                return new RestHighLevelClient(
                        RestClient
                                .builder(httpHosts)
                                .setHttpClientConfigCallback(
                                        httpClientBuilder -> httpClientBuilder
                                                .setDefaultCredentialsProvider(credentialsProvider)));
            }
            //无用户名密码
            return new RestHighLevelClient(RestClient.builder(httpHosts));
        } else {
            RestHighLevelClient restHighLevelClient = ElasticSearchManager
                    .getInstance()
                    .getConnection(esSource)
                    .getResource();
            if (Objects.isNull(restHighLevelClient)) {
                throw new DtLoaderException("No database connection available");
            }
            return restHighLevelClient;
        }
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        String currentIndex = (String) currentObject;
        IndexProperties indexProperties = EsUtil.queryIndexProp(currentIndex, lowClient);
        List<AliasEntity> aliasList = EsUtil.queryAliases(currentIndex, lowClient);
        List<ColumnEntity> columnList = EsUtil.queryColumns(currentIndex, lowClient);
        MetaDataEsEntity metaDataEsEntity = new MetaDataEsEntity();
        metaDataEsEntity.setTableName(currentIndex);
        metaDataEsEntity.setSchema(currentDatabase);
        metaDataEsEntity.setColumns(columnList);
        metaDataEsEntity.setTableProperties(indexProperties);
        metaDataEsEntity.setAliasList(aliasList);
        metaDataEsEntity.setQuerySuccess(true);

        return metaDataEsEntity;
    }

    /**
     * 返回es集群下的所有索引
     *
     * @return 索引列表
     */
    @Override
    protected List<Object> showTables() {
        List<Object> indexNameList = new ArrayList<>();
        String[] indexArr = EsUtil.queryIndicesByCat(lowClient);
        /*
         * 字符串数组indexArr的数据结构如下，数组下标positon对应具体变量位置,此处需循环读取所有的index，即下标为position+differrence*n的数组的值
         * 0      1      2        3                      4   5   6          7            8          9
         * health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
         * yellow open   megacorp LYXJZVslTaiTOtQzVFqfLg   5   1          0            0      1.2kb          1.2kb
         * yellow open   test     oixXJg2jThG82H_Y4ZpgcA   5   1          0            0      1.2kb          1.2kb
         */
        int position = 2, difference = 10;
        while (position < indexArr.length) {
            indexNameList.add(indexArr[position]);
            position += difference;
        }

        return indexNameList;
    }

    @Override
    public void close() {
        if (lowClient != null) {
            try {
                lowClient.close();
            } catch (IOException e) {
                throw new DtLoaderException("close es client error", e);
            }
        }
    }

    /**
     * 切分多url，并进行网络连通性验证
     *
     * @param address url
     * @return httpHost
     */
    private List<HttpHost> splitAddress(String address) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            TelUtil.checkTelnetAddr(add);
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), KEY_HTTPS));
        }
        return httpHostList;
    }
}
