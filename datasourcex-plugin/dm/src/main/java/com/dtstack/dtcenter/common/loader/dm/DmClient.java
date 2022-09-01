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

package com.dtstack.dtcenter.common.loader.dm;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.SearchUtil;
import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.client.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DmSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:19 2020/4/17
 * @Description：达梦客户端
 */
@Slf4j
public class DmClient extends AbsRdbmsClient {

    private static final String DM_ALL_DATABASES = "SELECT DISTINCT OWNER FROM SYS.DBA_TABLES WHERE TABLESPACE_NAME = 'MAIN'";

    private static final String CREATE_TABLE_SQL = "select dbms_metadata.get_ddl(OBJECT_TYPE => 'TABLE',\n" +
            "NAME=>upper('%s'),SCHNAME => '%s')";

    // DMDB 获取指定schema下的表
    private static final String SHOW_TABLE_BY_SCHEMA_SQL = " SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '%s' %s ";

    // DMDB 获取指定schema下的视图
    private static final String SHOW_VIEW_BY_SCHEMA_SQL = " UNION SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = '%s' %s ";

    // DMDB 获取所有的schema下所有表 ： 新sql
    private static final String SHOW_ALL_TABLE_SQL = "SELECT '\"'||OWNER||'\"'||'.'||'\"'||TABLE_NAME||'\"' AS TABLE_NAME FROM ALL_TABLES WHERE OWNER != 'SYS' %s ";

    // DMDB 获取所有的schema下所有视图 ： 新sql
    private static final String SHOW_ALL_VIEW_SQL = " UNION SELECT '\"'||OWNER||'\"'||'.'||'\"'||VIEW_NAME||'\"' AS TABLE_NAME FROM ALL_VIEWS WHERE OWNER != 'SYS' %s ";

    // 表查询基础sql
    private static final String TABLE_BASE_SQL = "SELECT TABLE_NAME FROM (%s) WHERE 1 = 1 %s ";

    // 表名正则匹配模糊查询，忽略大小写
    private static final String TABLE_SEARCH_SQL = " AND REGEXP_LIKE (TABLE_NAME, '%s', 'i') ";

    // 视图正则匹配模糊查询，忽略大小写
    private static final String VIEW_SEARCH_SQL = " AND REGEXP_LIKE (VIEW_NAME, '%s', 'i') ";

    // 限制条数语句
    private static final String LIMIT_SQL = " AND ROWNUM <= %s ";

    // 获取当前版本号
    private static final String SHOW_VERSION = "select BANNER from v$version";

    @Override
    protected ConnFactory getConnFactory() {
        return new DmConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.DMDB;
    }

    @Override
    public List<String> getTableList(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        return getTableListBySchema(sourceDTO, queryDTO);
    }

    @Override
    public IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        DmSourceDTO dmSourceDTO = (DmSourceDTO) source;
        DmDownloader dmDownloader = new DmDownloader(getCon(dmSourceDTO), queryDTO.getSql(), dmSourceDTO.getSchema());
        dmDownloader.configure();
        return dmDownloader;
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) {
        DmSourceDTO dmSourceDTO = (DmSourceDTO) source;
        queryDTO.setSql(String.format(CREATE_TABLE_SQL,queryDTO.getTableName(), dmSourceDTO.getSchema()));
        return super.getCreateTableSql(source,queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getShowDbSql() {
        return DM_ALL_DATABASES;
    }

    @Override
    protected String getVersionSql() {
        return SHOW_VERSION;
    }

    /**
     * 查询指定schema下的表，如果没有填schema，默认使用当前schema：支持条数限制、正则匹配
     *
     * @param sourceDTO 数据源信息
     * @param queryDTO  查询条件
     * @return 对应的sql语句
     */
    @Override
    protected String getTableBySchemaSql(ISourceDTO sourceDTO, SqlQueryDTO queryDTO) {
        // 构造表名模糊查询和条数限制sql
        String tableConstr = buildSearchSql(TABLE_SEARCH_SQL, queryDTO.getTableNamePattern(), queryDTO.getLimit());
        // 构造视图模糊查询和条数限制sql
        String viewConstr = buildSearchSql(VIEW_SEARCH_SQL, queryDTO.getTableNamePattern(), queryDTO.getLimit());
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) sourceDTO;
        String schema = StringUtils.isNotBlank(queryDTO.getSchema()) ? queryDTO.getSchema() : rdbmsSourceDTO.getSchema();
        // schema若为空，则查询所有schema下的表
        String searchSql;
        if (StringUtils.isBlank(schema)) {
            log.info("schema is null，get all table！");
            searchSql = queryDTO.getView() ? String.format(SHOW_ALL_TABLE_SQL + SHOW_ALL_VIEW_SQL, tableConstr, viewConstr) : String.format(SHOW_ALL_TABLE_SQL, tableConstr);
        } else {
            searchSql = queryDTO.getView() ?  String.format(SHOW_TABLE_BY_SCHEMA_SQL + SHOW_VIEW_BY_SCHEMA_SQL, schema, tableConstr, schema, viewConstr) : String.format(SHOW_TABLE_BY_SCHEMA_SQL, schema, tableConstr);
        }
        log.info("current used schema：{}", schema);

        return String.format(TABLE_BASE_SQL, searchSql, tableConstr);
    }

    /**
     * 构造模糊查询、条数限制sql
     *
     * @param tableSearchSql   查询 sql
     * @param tableNamePattern 模糊查询
     * @param limit            限制条数
     * @return sql 结果
     */
    private String buildSearchSql(String tableSearchSql, String tableNamePattern, Integer limit) {
        StringBuilder constr = new StringBuilder();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(tableNamePattern)) {
            constr.append(String.format(tableSearchSql, tableNamePattern));
        }
        if (Objects.nonNull(limit)) {
            constr.append(String.format(LIMIT_SQL, limit));
        }
        return constr.toString();
    }
}
