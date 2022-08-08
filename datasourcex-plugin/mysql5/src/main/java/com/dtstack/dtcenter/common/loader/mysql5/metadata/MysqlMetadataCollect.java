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

package com.dtstack.dtcenter.common.loader.mysql5.metadata;

import com.dtstack.dtcenter.common.loader.common.utils.DateUtil;
import com.dtstack.dtcenter.common.loader.mysql5.MysqlConnFactory;
import com.dtstack.dtcenter.common.loader.mysql5.metadata.constants.MysqlMetadataCons;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.RdbmsMetaDataCollect;
import com.dtstack.dtcenter.common.loader.rdbms.metadata.constants.RdbCons;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mysql.IndexEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mysql.MetadataMysqlEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.mysql.MysqlTableEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.ColumnEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.MetadataRdbEntity;
import com.dtstack.dtcenter.loader.dto.metadata.entity.rdb.TableEntity;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * mysql metadata collect
 *
 * @author ：wangchuan
 * date：Created in 下午10:57 2022/4/6
 * company: www.dtstack.com
 */
public class MysqlMetadataCollect extends RdbmsMetaDataCollect {

    @Override
    protected MetadataRdbEntity createMetadataRdbEntity() {
        MetadataMysqlEntity metadataMysqlEntity = new MetadataMysqlEntity();
        try {
            metadataMysqlEntity.setIndexEntities(queryIndex());
            metadataMysqlEntity.setTableProperties(createTableEntity());
            metadataMysqlEntity.setColumns(queryColumn(null));
        } catch (Exception e) {
            throw new DtLoaderException("create mysql rdb entity error.", e);
        }
        return metadataMysqlEntity;
    }

    /**
     * 查询索引
     *
     * @return 索引实例集合
     */
    private List<IndexEntity> queryIndex() throws SQLException {
        List<IndexEntity> indexEntities = new LinkedList<>();
        String sql = String.format(MysqlMetadataCons.SQL_QUERY_INDEX, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                IndexEntity entity = new IndexEntity();
                entity.setIndexName(rs.getString(MysqlMetadataCons.RESULT_KEY_NAME));
                entity.setColumnName(rs.getString(MysqlMetadataCons.RESULT_COLUMN_NAME));
                entity.setIndexType(rs.getString(MysqlMetadataCons.RESULT_INDEX_TYPE));
                entity.setIndexComment(rs.getString(MysqlMetadataCons.RESULT_INDEX_COMMENT));
                indexEntities.add(entity);
            }
        }
        return indexEntities;
    }

    /**
     * 创建表信息实体
     *
     * @return 表信息实体
     */
    private TableEntity createTableEntity() {
        MysqlTableEntity entity = new MysqlTableEntity();
        String sql = String.format(MysqlMetadataCons.SQL_QUERY_TABLE_INFO, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                entity.setTableType(MysqlMetadataCons.RESULT_TABLE_TYPE);
                entity.setComment(rs.getString(MysqlMetadataCons.RESULT_TABLE_COMMENT));
                entity.setCreateTime(DateUtil.stringToDate(rs.getString(MysqlMetadataCons.RESULT_CREATE_TIME)).getTime());
                entity.setTotalSize(rs.getLong(MysqlMetadataCons.RESULT_DATA_LENGTH));
                entity.setRows(rs.getLong(MysqlMetadataCons.RESULT_ROWS));
                entity.setEngine(rs.getString(MysqlMetadataCons.RESULT_ENGINE));
                entity.setRowFormat(rs.getString(MysqlMetadataCons.RESULT_ROW_FORMAT));
            }
        } catch (SQLException e) {
            throw new DtLoaderException(String.format("execute sql [%s] error", sql), e);
        }
        return entity;
    }

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(MysqlMetadataCons.SQL_SWITCH_DATABASE, currentDatabase));
    }

    @Override
    protected void setPrecisionAndDigital(String schema, ResultSet resultSet, ColumnEntity columnEntity) throws SQLException {
        Object digits = resultSet.getObject(RdbCons.RESULT_DECIMAL_DIGITS);
        Object precision = resultSet.getObject(RdbCons.RESULT_COLUMN_SIZE);
        if (digits != null && precision != null) {
            columnEntity.setDigital(resultSet.getInt(RdbCons.RESULT_DECIMAL_DIGITS));
            columnEntity.setPrecision(resultSet.getInt(RdbCons.RESULT_COLUMN_SIZE));
        }
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlConnFactory();
    }
}
