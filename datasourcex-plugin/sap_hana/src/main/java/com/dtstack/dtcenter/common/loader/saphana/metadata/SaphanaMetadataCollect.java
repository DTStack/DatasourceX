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

package com.dtstack.dtcenter.common.loader.saphana.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.saphana.SapHanaConnFactory;
import com.dtstack.dtcenter.common.loader.saphana.metadata.constants.SQLTemplate;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.saphana.IndexEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.saphana.MetadatahanaEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.saphana.SaphanaTableEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * saphana 元数据拾取器
 *
 * @author by zhiyi
 * @date 2022/4/8 10:07 上午
 */
@Slf4j
public class SaphanaMetadataCollect extends RdbmsMetaDataCollect {

    private String databaseName;

    @Override
    protected void openConnection() {
        try {
            if (connection == null) {
                connection = connFactory.getConn(sourceDTO);
                statement = connection.createStatement();
            }
            // SAP hana can not change database during a connection,
            // so method 'switchDataBase' cannot be implemented.
            databaseName = queryDatabase();
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadatahanaEntity entity = new MetadatahanaEntity();
        entity.setTableName((String) currentObject);
        // Actually, currentDatabase is schema name,
        // true database name need to query.
        entity.setSchema(currentDatabase);
        entity.setDatabaseName(databaseName);
        entity.setIndexEntities(queryIndexes());
        entity.setColumns(queryColumn(null));
        entity.setTableProperties(queryTableInfo());
        return entity;
    }


    @Override
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        String sql = String.format(SQLTemplate.QUERY_TABLES, currentDatabase);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getObject(1));
            }
        } catch (Exception e) {
            log.error("query tables error", e);
        }
        return tables;
    }

    @Override
    public List<? extends ColumnEntity> queryColumn(String schema) {
        List<ColumnEntity> columns = new ArrayList<>();
        String sql = String.format(SQLTemplate.QUERY_COLUMNS, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setName(rs.getString(1));
                columnEntity.setIndex(rs.getInt(2));
                columnEntity.setType(rs.getString(3));
                columnEntity.setLength(rs.getInt(4));
                columnEntity.setDefaultValue(rs.getString(5));
                columnEntity.setComment(rs.getString(6));
                columns.add(columnEntity);
            }
        } catch (Exception e) {
            log.error("query columns error", e);
        }
        return columns;
    }


    private List<IndexEntity> queryIndexes() {
        List<IndexEntity> indexes = new ArrayList<>();
        String sql = String.format(SQLTemplate.QUERY_INDEXES, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                IndexEntity indexEntity = new IndexEntity();
                indexEntity.setIndexName(rs.getString(1));
                indexEntity.setColumnName(rs.getString(2));
                indexEntity.setIndexType(rs.getString(3));
            }
        } catch (Exception e) {
            log.error("query index error", e);
        }
        return indexes;
    }

    private String queryDatabase() {
        try (ResultSet rs = statement.executeQuery(SQLTemplate.QUERY_DATABASE)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            log.error("query current database name failed.", e);
        }
        return "";
    }

    private SaphanaTableEntity queryTableInfo() {
        SaphanaTableEntity tableEntity = new SaphanaTableEntity();
        String sql = String.format(SQLTemplate.QUERY_TABLE, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                tableEntity.setTableName(rs.getString(1));
                tableEntity.setRows(rs.getLong(2));
                tableEntity.setTotalSize(rs.getLong(3));
                tableEntity.setTableType(rs.getString(4));
                tableEntity.setComment(rs.getString(5));
            }
        } catch (SQLException e) {
            log.error("query table info failed.", e);
        }

        String createTimeSql = String.format(SQLTemplate.QUERY_TABLE_CREATE_TIME, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(createTimeSql)) {
            if (rs.next()) {
                tableEntity.setCreateTime(DateUtil.stringToDate(rs.getString(1)).getTime());
            }

        } catch (SQLException e) {
            log.error("query table create_time failed.", e);
        }
        return tableEntity;
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new SapHanaConnFactory();
    }
}
