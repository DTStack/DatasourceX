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

package com.dtstack.dtcenter.common.loader.hbase.restful;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.utils.HexUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hbase.restful.cons.FilterType;
import com.dtstack.dtcenter.common.loader.hbase.restful.cons.HttpApi;
import com.dtstack.dtcenter.common.loader.hbase.restful.cons.NormalCons;
import com.dtstack.dtcenter.common.loader.hbase.restful.entity.FilterDTO;
import com.dtstack.dtcenter.common.loader.hbase.restful.util.JsonUtils;
import com.dtstack.dtcenter.common.loader.restful.http.HbaseHttpClient;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClient;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClientFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.PageFilter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter;
import com.dtstack.dtcenter.loader.dto.filter.TimestampFilter;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.HbaseRestfulSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * hbase restful客户端
 *
 * @author luming
 * @date 2022/4/27
 */
@Slf4j
public class HbaseRestfulClient extends AbsNoSqlClient {

    @Override
    public Boolean testCon(ISourceDTO source) {
        HbaseRestfulSourceDTO sourceDTO = initSource(source);
        sourceDTO.setUrl(String.format(HttpApi.GET_CLUSTER_STATUS, sourceDTO.getUrl()));
        checkKerberos(sourceDTO);

        return KerberosLoginUtil.loginWithUGI(sourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try (HttpClient httpClient =
                                 HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
                        AssertUtils.httpSuccess(httpClient.get());
                        return true;
                    } catch (IOException e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }
                }
        );
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        HbaseRestfulSourceDTO sourceDTO = initSource(source);
        sourceDTO.setUrl(String.format(HttpApi.GET_TABLES, sourceDTO.getUrl()));
        checkKerberos(sourceDTO);

        return oriGetTables(sourceDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        AssertUtils.notBlank(queryDTO.getSchema(), "hbase namespace can't be null");
        HbaseRestfulSourceDTO sourceDTO = initSource(source);
        sourceDTO.setUrl(
                String.format(HttpApi.GET_TABLES_BY_SCHEMA, sourceDTO.getUrl(), queryDTO.getSchema()));
        checkKerberos(sourceDTO);

        return oriGetTables(sourceDTO);
    }


    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        AssertUtils.notBlank(queryDTO.getTableName(), "hbase tableName can't be null");
        HbaseRestfulSourceDTO sourceDTO = initSource(source);
        sourceDTO.setUrl(
                String.format(HttpApi.GET_COLUMN_FAMILY, sourceDTO.getUrl(), queryDTO.getTableName()));
        checkKerberos(sourceDTO);

        return KerberosLoginUtil.loginWithUGI(sourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<ColumnMetaDTO>>) () -> {
                    try (HttpClient httpClient =
                                 HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
                        Response response = httpClient.get();
                        AssertUtils.httpSuccess(response);
                        return JsonUtils.parseColumns(response.getContent());
                    } catch (IOException e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }
                }
        );
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        AssertUtils.notBlank(queryDTO.getTableName(), "hbase tableName can't be null");

        //首先获取scannerId
        HbaseRestfulSourceDTO sourceDTO = initSource(source, 1);
        sourceDTO.setUrl(
                String.format(HttpApi.GET_SCANNER, sourceDTO.getUrl(), queryDTO.getTableName()));
        checkKerberos(sourceDTO);

        //需要实现的过滤
        //1.表名
        List<FilterDTO> filters = Lists.newArrayList();
        //2.列名
        filters.addAll(buildQualifierFilters(queryDTO.getColumns()));
        //3.自定义filter过滤
        filters.addAll(convertFilters(queryDTO.getHbaseFilter()));
        //4.limit,用pageFilter ✓
        if (Objects.nonNull(queryDTO.getLimit())) {
            filters.add(setLimit(queryDTO.getLimit()));
        }

        return KerberosLoginUtil.loginWithUGI(sourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<Map<String, Object>>>) () -> {
                    String scannerId;
                    List<Map<String, Object>> results = Lists.newArrayList();

                    try (HbaseHttpClient httpClient =
                                 (HbaseHttpClient) HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
                        String format = String.format(
                                NormalCons.SCANNER_TEMPLATE,
                                Objects.isNull(queryDTO.getFetchSize()) ? 1000 : queryDTO.getFetchSize(),
                                JSON.toJSONString(filters));
                        Response response = httpClient.hbasePut(format);
                        AssertUtils.httpSuccess(response, 201);
                        scannerId = response.getContent();
                    } catch (Exception e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }

                    //5.返回拼装数据
                    //todo：是否需要循环获取，由于有Batch
                    HbaseRestfulSourceDTO scanSource = initSource(source);
                    scanSource.setUrl(String.format(
                            HttpApi.QUERY_BY_SCANNER, scanSource.getUrl(), queryDTO.getTableName(), scannerId));
                    try (HbaseHttpClient httpClient =
                                 (HbaseHttpClient) HttpClientFactory.createHttpClientAndStart(scanSource)) {
                        Response response = httpClient.get();
                        AssertUtils.notNull(response, "response is null");

                        switch (response.getStatusCode()) {
                            case 204:
                                break;
                            case 200:
                                results.addAll(JsonUtils.parseScanResult(response.getContent()));
                                break;
                            default:
                                throw new DtLoaderException("hbase restful scan error." + response.getErrorMsg());
                        }
                    } catch (Exception e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    } finally {
                        //6.最后销毁scanner资源
                        HbaseRestfulSourceDTO delSource = initSource(source, 1);
                        delSource.setUrl(String.format(
                                HttpApi.DELETE_SCANNER, delSource.getUrl(), queryDTO.getTableName(), scannerId));
                        try (HbaseHttpClient client =
                                     (HbaseHttpClient) HttpClientFactory.createHttpClientAndStart(scanSource)) {
                            Response delRes = client.delete(null, null, null);
                            AssertUtils.httpSuccess(delRes, 200);
                        } catch (Exception e) {
                            log.error("delete scanner resource error.", e);
                        }
                    }

                    return results;
                }
        );
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        //只有previewNum和tableName需要，其余参数不要，防止干扰查询
        List<Map<String, Object>> previews = executeQuery(
                source,
                SqlQueryDTO.builder().tableName(queryDTO.getTableName()).limit(queryDTO.getPreviewNum()).build());
        return previews.stream()
                .map(row -> new ArrayList<Object>(Collections.singleton(row)))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        HbaseRestfulSourceDTO sourceDTO = initSource(source);
        sourceDTO.setUrl(
                String.format(HttpApi.GET_NAMESPACES, sourceDTO.getUrl()));
        checkKerberos(sourceDTO);

        return KerberosLoginUtil.loginWithUGI(sourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<String>>) () -> {
                    try (HttpClient httpClient =
                                 HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
                        Response response = httpClient.get();
                        AssertUtils.httpSuccess(response);
                        return JsonUtils.parseNamespaces(response.getContent());
                    } catch (IOException e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }
                }
        );
    }

    /**
     * 将入参列族信息转换为hbase restful的QualifierFilter
     *
     * @param columns 列族信息
     * @return filters
     */
    private List<FilterDTO> buildQualifierFilters(List<String> columns) {
        List<String> qualifiers = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(columns)) {
            for (String column : columns) {
                String[] familyAndQualifier = column.split(":");
                if (familyAndQualifier.length < 2) {
                    continue;
                }
                qualifiers.add(familyAndQualifier[1]);
            }
        }
        if (CollectionUtils.isEmpty(qualifiers)) {
            return Lists.newArrayList();
        }

        return qualifiers.stream()
                .map(qualifier -> FilterDTO.builder()
                        .type(FilterType.QUALIFIER_FILTER.getName())
                        .op(CompareOp.EQUAL.name())
                        .comparator(
                                FilterDTO.Comparator.builder()
                                        .type("BinaryComparator")
                                        .value(HexUtil.base64En(qualifier))
                                        .build())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * 将入参的hbaseFilter转换为hbase restful需要的filters
     * 暂时支持四种filter类型的转换：
     * RowFilter， TimestampFilter， PageFilter，SingleColumnValueFilter
     *
     * @param hbaseFilters 入参filters
     * @return filters
     */
    private List<FilterDTO> convertFilters(List<Filter> hbaseFilters) {
        if (CollectionUtils.isNotEmpty(hbaseFilters)) {
            return hbaseFilters.stream()
                    .map(hbaseFilter -> {
                        if (hbaseFilter instanceof RowFilter) {
                            RowFilter rowFilter = (RowFilter) hbaseFilter;
                            return FilterDTO.builder()
                                    .type(FilterType.getType(rowFilter.getFilterType()))
                                    .op(rowFilter.getCompareOp().name())
                                    .comparator(
                                            FilterDTO.Comparator.builder()
                                                    .type("BinaryComparator")
                                                    .value(HexUtil.base64En(new String(rowFilter.getComparator().getValue())))
                                                    .build())
                                    .build();
                        }
                        if (hbaseFilter instanceof TimestampFilter) {
                            //暂时未找到range的方法
                            return null;
                        }
                        if (hbaseFilter instanceof PageFilter) {
                            //pageFilter这里不提供作用，由limit实现
                            return null;
                        }
                        if (hbaseFilter instanceof SingleColumnValueFilter) {
                            SingleColumnValueFilter valueFilter = (SingleColumnValueFilter) hbaseFilter;
                            return FilterDTO.builder()
                                    .family(HexUtil.base64En(new String(valueFilter.getColumnFamily())))
                                    .qualifier(HexUtil.base64En(new String(valueFilter.getColumnQualifier())))
                                    .type(FilterType.getType(valueFilter.getFilterType()))
                                    .op(valueFilter.getCompareOp().name())
                                    .comparator(
                                            FilterDTO.Comparator.builder()
                                                    .type("BinaryComparator")
                                                    .value(HexUtil.base64En(new String(valueFilter.getComparator().getValue())))
                                                    .build())
                                    .build();
                        }

                        throw new DtLoaderException("hbase restful don't support this filter type now");
                    })
                    .collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    /**
     * limit参数用pageFilter实现
     *
     * @param limit 限制返回条数
     * @return pageFilter
     */
    private FilterDTO setLimit(int limit) {
        return FilterDTO.builder()
                .type(FilterType.PAGE_FILTER.getName())
                .value(String.valueOf(limit))
                .build();
    }

    /**
     * 获取默认库和指定库表方法抽出来的
     *
     * @param sourceDTO 数据源信息
     * @return tables
     */
    private List<String> oriGetTables(HbaseRestfulSourceDTO sourceDTO) {
        return KerberosLoginUtil.loginWithUGI(sourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<List<String>>) () -> {
                    try (HttpClient httpClient =
                                 HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
                        Response response = httpClient.get();
                        AssertUtils.httpSuccess(response);
                        return JsonUtils.parseTables(response.getContent());
                    } catch (IOException e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }
                }
        );
    }


    private HbaseRestfulSourceDTO initSource(ISourceDTO source) {
        return initSource(source, 0);
    }

    /**
     * 入参对象执行深拷贝，防止setUrl出错
     *
     * @param source 数据源信息
     * @param type   请求类型： 0-普通请求，1-scanner相关请求
     * @return 拷贝后对象
     */
    private HbaseRestfulSourceDTO initSource(ISourceDTO source, int type) {
        HbaseRestfulSourceDTO sourceDTO = (HbaseRestfulSourceDTO) source;
        //scanner相关请求需要设置为text类型，其他请求可以返回json类型
        Map<String, String> headers = sourceDTO.getHeaders() == null ? Maps.newHashMap() : sourceDTO.getHeaders();
        if (type == 0) {
            headers.put("Accept", "application/json");
        } else if (type == 1) {
            headers.put("Accept", "text/xml");
            headers.put("Content-Type", "text/xml");
        }
        String url = sourceDTO.getUrl();
        if (StringUtils.isNotEmpty(sourceDTO.getKnoxProxy())){
            JSONObject knoxJson = JSON.parseObject(sourceDTO.getKnoxProxy());
            url = knoxJson.getString("url");
            String username = knoxJson.getString("username");
            String password = knoxJson.getString("password");
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
            String authHeader = "Basic " + new String(encodedAuth);
            // 将验证信息放入到 Header
            headers.put(HttpHeaders.AUTHORIZATION, authHeader);
        }

        return HbaseRestfulSourceDTO.builder()
                .url(url)
                .sslConfig(sourceDTO.getSslConfig())
                .sftpConf(sourceDTO.getSftpConf())
                .connectTimeout(sourceDTO.getConnectTimeout())
                .headers(headers)
                .protocol(sourceDTO.getProtocol())
                .socketTimeout(sourceDTO.getSocketTimeout())
                .tenantId(sourceDTO.getTenantId())
                .kerberosConfig(sourceDTO.getKerberosConfig())
                .poolConfig(sourceDTO.getPoolConfig())
                .build();
    }

    /**
     * Kerberos必须参数的校验和填充
     *
     * @param source 数据源信息
     */
    private void checkKerberos(HbaseRestfulSourceDTO source) {
        Map<String, Object> kerberosConfig = source.getKerberosConfig();
        if (MapUtils.isNotEmpty(kerberosConfig)) {
            if (!kerberosConfig.containsKey(HadoopConfTool.HBASE_MASTER_PRINCIPAL)) {
                throw new DtLoaderException(String.format("HBASE must setting %s ", HadoopConfTool.HBASE_MASTER_PRINCIPAL));
            }

            if (!kerberosConfig.containsKey(HadoopConfTool.HBASE_REGION_PRINCIPAL)) {
                log.info("setting hbase.regionserver.kerberos.principal 为 {}", kerberosConfig.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
                kerberosConfig.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, kerberosConfig.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
            }
        }

        log.info("get Hbase connection, url : {}, kerberosConfig : {}", source.getUrl(), source.getKerberosConfig());
    }
}
