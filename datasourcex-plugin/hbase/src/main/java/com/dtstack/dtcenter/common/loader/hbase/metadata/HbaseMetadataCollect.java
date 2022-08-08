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

package com.dtstack.dtcenter.common.loader.hbase.metadata;

import com.dtstack.dtcenter.common.loader.hbase.HbaseConnFactory;
import com.dtstack.dtcenter.common.loader.hbase.metadata.cons.HbaseCons;
import com.dtstack.dtcenter.common.loader.hbase.metadata.cons.SizeUnitType;
import com.dtstack.dtcenter.common.loader.hbase.metadata.entity.HbaseColumnEntity;
import com.dtstack.dtcenter.common.loader.hbase.metadata.entity.HbaseTableEntity;
import com.dtstack.dtcenter.common.loader.hbase.metadata.entity.MetadataHbaseEntity;
import com.dtstack.dtcenter.common.loader.hbase.metadata.util.ZkHelper;
import com.dtstack.dtcenter.common.loader.hbase.pool.HbasePoolManager;
import com.dtstack.dtcenter.common.loader.metadata.collect.AbstractMetaDataCollect;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.metadata.entity.MetadataEntity;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * hbase元数据收集器
 *
 * @author luming
 * @date 2022/4/13
 */
@Slf4j
public class HbaseMetadataCollect extends AbstractMetaDataCollect {

    private Connection hbaseConn;

    private Admin admin;
    /**
     * hbase 建表时间的集合
     */
    protected Map<String, Long> createTimeMap;
    /**
     * hbase 表大小的集合
     */
    protected Map<String, Integer> tableSizeMap;

    @Override
    protected void openConnection() {
        hbaseConn = HbaseConnFactory.getHbaseConn((HbaseSourceDTO) sourceDTO);
        try {
            admin = hbaseConn.getAdmin();
        } catch (IOException e) {
            throw new DtLoaderException("hbase getAdmin error.", e);
        }

        createTimeMap = queryCreateTimeMap(sourceDTO);
        tableSizeMap = generateTableSizeMap();

        if (CollectionUtils.isEmpty(tableList)) {
            tableList = showTables();
        }
    }

    @Override
    protected MetadataEntity createMetadataEntity() {
        MetadataHbaseEntity entity = new MetadataHbaseEntity();
        entity.setTableName((String) currentObject);
        entity.setSchema(currentDatabase);
        String tableName = String.format("%s:%s", currentDatabase, currentObject);
        entity.setColumns(queryColumnList(tableName));
        entity.setTableProperties(queryTableProperties(tableName));
        entity.setQuerySuccess(true);
        return entity;
    }

    /**
     * 获取列族信息
     *
     * @return 列族
     */
    protected List<HbaseColumnEntity> queryColumnList(String tableName) {
        List<HbaseColumnEntity> columnList = new ArrayList<>();
        try {
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
            for (HColumnDescriptor column : columnDescriptors) {
                HbaseColumnEntity hbaseColumnEntity = new HbaseColumnEntity();
                hbaseColumnEntity.setColumnFamily(column.getNameAsString());
                columnList.add(hbaseColumnEntity);
            }
        } catch (IOException e) {
            throw new DtLoaderException("query columnList failed.", e);
        }
        return columnList;
    }

    protected HbaseTableEntity queryTableProperties(String tableName) {
        HbaseTableEntity hbaseTableEntity = new HbaseTableEntity();
        try {
            HTableDescriptor table = admin.getTableDescriptor(TableName.valueOf(tableName));
            List<HRegionInfo> regionInfos = admin.getTableRegions(table.getTableName());
            hbaseTableEntity.setRegionCount(regionInfos.size());
            //统一表大小单位为字节
            String tableSize = SizeUnitType.covertUnit(SizeUnitType.MB, SizeUnitType.B, Long.valueOf(tableSizeMap.get(table.getNameAsString())));
            hbaseTableEntity.setTotalSize(Long.valueOf(tableSize));
            hbaseTableEntity.setCreateTime(createTimeMap.get(table.getNameAsString()));
            //这里的table带了schema
            if (tableName.contains(HbaseCons.COLON_SYMBOL)) {
                tableName = tableName.split(HbaseCons.COLON_SYMBOL)[1];
            }
            hbaseTableEntity.setTableName(tableName);
            hbaseTableEntity.setNamespace(currentDatabase);
        } catch (Exception e) {
            throw new DtLoaderException("query tableProperties failed.", e);
        }
        return hbaseTableEntity;
    }

    @Override
    protected List<Object> showTables() {
        List<Object> tableNameList = new LinkedList<>();
        try {
            HTableDescriptor[] tableNames = admin.listTableDescriptorsByNamespace(currentDatabase);
            for (HTableDescriptor table : tableNames) {
                TableName tableName = table.getTableName();
                // 排除系统表
                if (!tableName.isSystemTable()) {
                    //此时的表名带有namespace,需要去除
                    String tableWithNameSpace = tableName.getNameAsString();
                    if (tableWithNameSpace.contains(HbaseCons.COLON_SYMBOL)) {
                        tableWithNameSpace = tableWithNameSpace.split(HbaseCons.COLON_SYMBOL)[1];
                    }
                    tableNameList.add(tableWithNameSpace);
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(
                    String.format(
                            "current DbName = %s, query table list failed", currentDatabase), e);
        }
        return tableNameList;
    }

    @Override
    public void close() {
        try {
            admin.close();
            hbaseConn.close();
        } catch (Exception e) {
            throw new DtLoaderException("close hbase error.", e);
        }
    }

    /**
     * 获取表的region大小的总和即为表的存储大小，误差最大为1M * regionSize
     *
     * @return 表名->size
     */
    private Map<String, Integer> generateTableSizeMap() {
        Map<String, Integer> sizeMap = new HashMap<>(16);
        ClusterStatus clusterStatus;
        try {
            clusterStatus = admin.getClusterStatus();
        } catch (IOException e) {
            throw new DtLoaderException("getClusterStatus error.", e);
        }

        for (ServerName serverName : clusterStatus.getServers()) {
            ServerLoad serverLoad = clusterStatus.getLoad(serverName);
            for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                RegionLoad regionLoad = entry.getValue();
                String regionName = new String(entry.getKey(), StandardCharsets.UTF_8);
                String[] regionSplits = regionName.split(HbaseCons.COMMA_SYMBOL);
                //regionSplits[0] 为table name
                int sumSize = sizeMap.getOrDefault(regionSplits[0], 0) + regionLoad.getStorefileSizeMB();
                sizeMap.put(regionSplits[0], sumSize);
            }
        }
        return sizeMap;
    }

    /**
     * 查询hbase表的创建时间
     * 如果zookeeper没有权限访问，返回空map
     *
     * @param sourceDTO hbaseSource
     * @return 表名与创建时间的映射
     */
    private Map<String, Long> queryCreateTimeMap(ISourceDTO sourceDTO) {
        HbaseSourceDTO hbaseSource = (HbaseSourceDTO) sourceDTO;
        String path = hbaseSource.getPath() + HbaseCons.ZK_TABLE;
        Map<String, Object> hbaseConfig = HbasePoolManager.sourceToMap(sourceDTO, SqlQueryDTO.builder().build());
        Map<String, Long> createTimeMap = new HashMap<>();
        ZooKeeper zooKeeper = null;

        try {
            zooKeeper = ZkHelper.createZkClient(
                    (String) hbaseConfig.get(HConstants.ZOOKEEPER_QUORUM), ZkHelper.DEFAULT_TIMEOUT);
            List<String> tables = ZkHelper.getChildren(zooKeeper, path);
            if (tables != null) {
                for (String table : tables) {
                    createTimeMap.put(
                            table, ZkHelper.getCreateTime(
                                    zooKeeper, path + HbaseCons.SINGLE_SLASH_SYMBOL + table));
                }
            }
        } catch (Exception e) {
            log.error("query createTime map failed, error ", e);
        } finally {
            ZkHelper.closeZooKeeper(zooKeeper);
        }
        return createTimeMap;
    }
}
