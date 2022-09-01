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

package com.dtstack.dtcenter.common.loader.phoenix5.metadata;

import com.dtstack.dtcenter.common.loader.metadata.core.MetadataBaseCollectSplit;
import com.dtstack.dtcenter.common.loader.phoenix5.PhoenixConnFactory;
import com.dtstack.dtcenter.common.loader.phoenix5.metadata.util.ZkHelper;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.metadata.MetadataCollectCondition;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.phoenix5.Phoenix5ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.phoenix5.Phoenix5TableEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.Phoenix5SourceDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooKeeper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.COLON_SYMBOL;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.KEY_DEFAULT;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.POINT_SYMBOL;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.RESULT_SET_TABLE_NAME;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.SINGLE_SLASH_SYMBOL;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.SQL_COLUMN;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.SQL_DEFAULT_COLUMN;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.SQL_DEFAULT_TABLE_NAME;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.constants.PhoenixMetadataCons.SQL_TABLE_NAME;
import static com.dtstack.dtcenter.common.loader.phoenix5.metadata.util.ZkHelper.APPEND_PATH;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.KEY_FALSE;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.KEY_TRUE;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.RESULT_COLUMN_NAME;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.RESULT_SET_ORDINAL_POSITION;
import static com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons.RESULT_TYPE_NAME;

/**
 * phoenix5 元数据采集器
 *
 * @author by zhiyi
 * @date 2022/4/11 3:00 下午
 */
@Slf4j
public class Phoenix5MetadataCollect extends RdbmsMetaDataCollect {

    /**
     * 表和创建时间集合
     */
    private Map<String, Long> createTimeMap;

    /**
     * phoenix url固定前缀
     */
    public static final String JDBC_PHOENIX_PREFIX = "jdbc:phoenix:";

    /**
     * 默认schema名称
     */
    public static final String DEFAULT_SCHEMA = "default";

    /**
     * phoenix 依赖的zookeeper集群信息
     */
    protected ZooKeeper zooKeeper;

    /**
     * phoenix在zookeeper上表空间路径
     */
    protected String path;

    /**
     * phoenix在zookeeper上znode
     */
    protected String zooKeeperPath;

    @Override
    public void init(ISourceDTO sourceDTO, MetadataCollectCondition metadataCollectCondition, MetadataBaseCollectSplit metadataBaseCollectSplit, Queue<MetadataEntity> metadataEntities) throws Exception {
        log.info("init collect Split start: {} ", metadataBaseCollectSplit);
        this.sourceDTO = sourceDTO;
        this.metadataCollectCondition = metadataCollectCondition;
        this.metadataBaseCollectSplit = metadataBaseCollectSplit;
        this.tableList = metadataBaseCollectSplit.getTableList();
        this.currentDatabase = metadataBaseCollectSplit.getDbName();
        this.zooKeeperPath = metadataCollectCondition.getPath();
        this.path = zooKeeperPath + APPEND_PATH;
        super.metadataEntities = metadataEntities;
        // 建立连接
        openConnection();
        this.iterator = tableList.iterator();
        IS_INIT.set(true);
        log.info("init collect Split end : {}", metadataBaseCollectSplit);
    }

    @Override
    protected void openConnection() {
        super.openConnection();
        if (createTimeMap == null) {
            String hosts = ((Phoenix5SourceDTO) sourceDTO)
                    .getUrl()
                    .substring(JDBC_PHOENIX_PREFIX.length())
                    .split("/")[0];
            createTimeMap = queryCreateTimeMap(hosts);
        }
    }

    @Override
    protected List<Object> showTables() {
        String sql;
        if (StringUtils.isBlank(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT)) {
            sql = SQL_DEFAULT_TABLE_NAME;
        } else {
            sql = String.format(SQL_TABLE_NAME, currentDatabase);
        }
        List<Object> table = new LinkedList<>();
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                table.add(resultSet.getString(RESULT_SET_TABLE_NAME));
            }
        } catch (SQLException e) {
            log.error("query table lists failed, {}", e.getMessage());
        }
        return table;
    }

    /**
     * 通过jdbc执行sql
     */
    protected ResultSet executeQuery0(String sql, Statement statement) {
        ResultSet resultSet = null;
        if (StringUtils.isNotBlank(sql)) {
            log.info("execute SQL : {}", sql);
            try {
                if (statement != null) {
                    resultSet = statement.executeQuery(sql);
                }
            } catch (SQLException e) {
                log.error("execute SQL failed : {}", e.getMessage());
            }

        }
        return resultSet;
    }

    /**
     * 查询表的创建时间
     * 如果zookeeper没有权限访问，返回空map
     *
     * @param hosts zookeeper地址
     * @return 表名与创建时间的映射
     */
    protected Map<String, Long> queryCreateTimeMap(String hosts) {
        Map<String, Long> createTimeMap = new HashMap<>(16);
        try {
            zooKeeper = ZkHelper.createZkClient(hosts, ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(zooKeeper, path);
            if (tables != null) {
                for (String table : tables) {
                    createTimeMap.put(table, ZkHelper.getCreateTime(zooKeeper, path + SINGLE_SLASH_SYMBOL + table));
                }
            }
        } catch (Exception e) {
            log.error("query createTime map failed, error {}", e.getMessage());
        } finally {
            ZkHelper.closeZooKeeper(zooKeeper);
        }
        return createTimeMap;
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        String tableName = (String) currentObject;
        MetadataRdbEntity metadataPhoenix5Entity = new MetadataRdbEntity();
        metadataPhoenix5Entity.setTableProperties(queryTableProp(tableName));
        metadataPhoenix5Entity.setColumns(queryColumn(null));
        return metadataPhoenix5Entity;
    }

    /**
     * 获取表级别的元数据信息
     *
     * @param tableName 表名
     * @return 表的元数据
     */
    public Phoenix5TableEntity queryTableProp(String tableName) {
        Phoenix5TableEntity phoenix5TableEntity = new Phoenix5TableEntity();
        if (StringUtils.isEmpty(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT)) {
            phoenix5TableEntity.setCreateTime(createTimeMap.get(tableName));
        } else {
            phoenix5TableEntity.setCreateTime(createTimeMap.get(currentDatabase + POINT_SYMBOL + tableName));
        }
        phoenix5TableEntity.setNamespace(currentDatabase);
        phoenix5TableEntity.setTableName(tableName);
        return phoenix5TableEntity;
    }

    /**
     * 获取列级别的元数据信息
     *
     * @return 列的元数据信息
     */
    @Override
    public List<ColumnEntity> queryColumn(String schema) {
        String tableName = (String) currentObject;
        List<ColumnEntity> columns = new LinkedList<>();
        String sql;
        if (isDefaultSchema()) {
            sql = String.format(SQL_DEFAULT_COLUMN, tableName);
        } else {
            sql = String.format(SQL_COLUMN, currentDatabase, tableName);
        }
        Map<String, String> familyMap = new HashMap<>(16);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                familyMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            log.error("query column information failed, {}", e.getMessage());
        }
        //default schema需要特殊处理
        String originSchema = currentDatabase;
        if (DEFAULT_SCHEMA.equalsIgnoreCase(originSchema)) {
            originSchema = null;
        }
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, originSchema, tableName, null)) {
            while (resultSet.next()) {
                Phoenix5ColumnEntity phoenix5ColumnEntity = new Phoenix5ColumnEntity();
                int index = resultSet.getInt(RESULT_SET_ORDINAL_POSITION);
                String family = familyMap.get(index);
                if (StringUtils.isBlank(family)) {
                    phoenix5ColumnEntity.setIsPrimaryKey(KEY_TRUE);
                    phoenix5ColumnEntity.setName(resultSet.getString(RESULT_COLUMN_NAME));
                } else {
                    phoenix5ColumnEntity.setIsPrimaryKey(KEY_FALSE);
                    phoenix5ColumnEntity.setName(family + COLON_SYMBOL + resultSet.getString(RESULT_COLUMN_NAME));
                }
                phoenix5ColumnEntity.setType(resultSet.getString(RESULT_TYPE_NAME));
                phoenix5ColumnEntity.setIndex(index);
                columns.add(phoenix5ColumnEntity);
            }
        } catch (SQLException e) {
            log.error("failed to get column information, {} ", e.getMessage());
        }
        return columns;
    }

    /**
     * phoenix 默认schema为空，在平台层设置值为default
     *
     * @return 是否为默认schema
     */
    public boolean isDefaultSchema() {
        return StringUtils.isBlank(currentDatabase) || StringUtils.endsWithIgnoreCase(currentDatabase, KEY_DEFAULT);
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new PhoenixConnFactory();
    }
}
