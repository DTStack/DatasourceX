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

package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.enums.EsCommandType;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:40 2020/2/26
 * @Description：查询信息
 */
@Data
@Builder
public class SqlQueryDTO {
    /**
     * 查询 SQL
     */
    private String sql;

    /**
     * schema/db
     */
    private String schema;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 表名称(正则)
     */
    private String tableNamePattern;

    /**
     * 表类型 部分支持，建议只使用 view 这个字段
     * {@link java.sql.DatabaseMetaData#getTableTypes()}
     */
    private String[] tableTypes;

    /**
     * 字段名称
     */
    private List<String> columns;

    /**
     * 分区字段:值,用于hive分区预览数据、hbase自定义查询
     */
    private Map<String, String> partitionColumns;

    /**
     * 是否需要视图表，默认 false 不过滤
     */
    private Boolean view;

    /**
     * 是否过滤分区字段，默认 false 不过滤
     */
    private Boolean filterPartitionColumns;

    /**
     * 预览条数，默认100
     */
    private Integer previewNum;

    /**
     * 预编译字段
     * todo 修改executeQuery方法为支持预编译
     * time :2020-08-04 15:49:00
     */
    private List<Object> preFields;

    /**
     * executorQuery查询超时时间,单位：秒
     */
    private Integer queryTimeout;

    /**
     * mongodb，executorQuery 分页查询，开始行
     */
    private Integer startRow;

    /**
     * mongodb，executorQuery 分页查询，限制条数
     */
    private Integer limit;

    /**
     * hbase过滤器，用户hbase自定义查询
     */
    private List<Filter> hbaseFilter;

    /**
     *  JDBC 每次读取数据的行数，使用 DBUtil.setFetchSize()
     */
    private Integer fetchSize;


    /**
     * solr 自定义查询
     */
    private SolrQueryDTO solrQueryDTO;

    /**
     * Elasticsearch 命令, 定义es操作类型
     * <b><b/>
     * <ul>
     *     <li>INSERT(0) insert 操作，插入时要指定_id</li>
     *     <li>UPDATE(1) _update 操作，指定_id</li>
     *     <li>DELETE(2) delete操作，删除单条数据要指定_id</li>
     *     <li>BULK(3) _bulk批量操作，默认请求/_bulk</li>
     * <ul/>
     * 默认执行POST请求，请求参数中的tableName作为esclient的endpoint
     * <br>
     * refer to {@link EsCommandType}
     */
    private Integer esCommandType;

    public Boolean getView() {
        if (ArrayUtils.isEmpty(getTableTypes())) {
            return Boolean.TRUE.equals(view);
        }

        return Arrays.stream(getTableTypes()).filter( type -> "VIEW".equalsIgnoreCase(type)).findFirst().isPresent();
    }

    public Integer getPreviewNum() {
        if (this.previewNum == null){
            return 100;
        }
        return previewNum;
    }

    public Boolean getFilterPartitionColumns() {
        return Boolean.TRUE.equals(filterPartitionColumns);
    }


}