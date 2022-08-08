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

package com.dtstack.dtcenter.common.loader.rdbms.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ConnectionInfo;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

/**
 * rdbms metadata collect
 *
 * @author ：wangchuan
 * date：Created in 下午10:35 2022/4/6
 * company: www.dtstack.com
 */
@Slf4j
public abstract class RdbmsMetaDataCollect extends AbstractMetaDataCollect {

    /**
     * 连接对象
     */
    protected Connection connection;

    /**
     * connection 初始化的 statement
     */
    protected Statement statement;

    /**
     * 连接信息 TODO
     */
    public ConnectionInfo connectionInfo;

    /**
     * 获取连接工厂
     */
    public final ConnFactory connFactory = getConnFactory();

    @Override
    protected void openConnection() {
        try {
            if (connection == null) {
                connection = connFactory.getConn(sourceDTO);
                statement = connection.createStatement();
            }
            switchDataBase();
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (Exception e) {
            throw new DtLoaderException("open connection error", e);
        }
    }

    /**
     * 切换 database
     */
    public void switchDataBase() throws SQLException {
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        MetadataRdbEntity entity = createMetadataRdbEntity();
        entity.setSchema(currentDatabase);
        entity.setTableName((String) currentObject);
        return entity;
    }

    /**
     * 创建关系型数据库 metadata 实体
     *
     * @return rdb MetadataRdbEntity
     */
    protected abstract MetadataRdbEntity createMetadataRdbEntity();

    @Override
    protected List<Object> showTables() {
        List<Object> tables = Lists.newArrayList();
        try (ResultSet resultSet = connection.getMetaData().getTables(currentDatabase, null, null, null)) {
            while (resultSet.next()) {
                tables.add(resultSet.getString(RdbCons.RESULT_TABLE_NAME));
            }
        } catch (SQLException e) {
            log.error("failed to query table, currentDb = {} ", currentDatabase);
            throw new DtLoaderException("show tables error " + e.getMessage(), e);
        }
        return tables;
    }

    protected List<? extends ColumnEntity> queryColumn(String schema) {
        List<ColumnEntity> columnEntities = Lists.newArrayList();
        String currentTable = (String) currentObject;
        if (currentTable.startsWith(".")) {
            currentTable = "\\" + currentTable;
        }
        try (ResultSet resultSet = connection.getMetaData().getColumns(currentDatabase, schema, currentTable, null);
             ResultSet primaryResultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schema, currentTable)) {
            Set<String> primaryColumns = Sets.newHashSet();
            while (primaryResultSet.next()) {
                primaryColumns.add(primaryResultSet.getString(RdbCons.RESULT_COLUMN_NAME));
            }
            while (resultSet.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setName(resultSet.getString(RdbCons.RESULT_COLUMN_NAME));
                columnEntity.setType(resultSet.getString(RdbCons.RESULT_TYPE_NAME));
                columnEntity.setIndex(resultSet.getInt(RdbCons.RESULT_ORDINAL_POSITION));
                columnEntity.setDefaultValue(resultSet.getString(RdbCons.RESULT_COLUMN_DEF));
                columnEntity.setNullAble(StringUtils.equals(resultSet.getString(RdbCons.RESULT_IS_NULLABLE), RdbCons.KEY_TES) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
                columnEntity.setComment(resultSet.getString(RdbCons.RESULT_REMARKS));
                columnEntity.setLength(resultSet.getInt(RdbCons.RESULT_COLUMN_SIZE));
                columnEntity.setPrimaryKey(primaryColumns.contains(columnEntity.getName()) ? RdbCons.KEY_TRUE : RdbCons.KEY_FALSE);
                setPrecisionAndDigital(schema, resultSet, columnEntity);
                columnEntities.add(columnEntity);
            }
        } catch (SQLException e) {
            log.error("queryColumn failed", e);
            throw new DtLoaderException("execute sql error", e);
        }
        return columnEntities;
    }

    protected void setPrecisionAndDigital(String schema, ResultSet resultSet, ColumnEntity columnEntity) throws SQLException {
        //do nothing
    }

    protected abstract ConnFactory getConnFactory();

    /**
     * 通过jdbc执行sql
     *
     * @param sql
     * @param statement
     * @return
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
                throw new DtLoaderException("execute SQL failed");
            }

        }
        return resultSet;
    }

    @Override
    public void close() {
        // 关闭 statement、connection 资源
        DBUtil.closeDBResources(null, statement, connection);
    }
}
