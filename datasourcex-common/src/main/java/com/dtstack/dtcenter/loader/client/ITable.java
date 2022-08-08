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

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.canal.BinlogCallBackLogDTO;
import com.dtstack.dtcenter.loader.dto.canal.BinlogConfigDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.rpc.annotation.RpcNodeSign;
import com.dtstack.rpc.annotation.RpcService;
import com.dtstack.rpc.enums.RpcRemoteType;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * <p>提供表相关操作接口</>
 *
 * @author ：wangchuan
 * date：Created in 10:17 上午 2020/11/12
 * company: www.dtstack.com
 */
@RpcService(rpcRemoteType = RpcRemoteType.DATASOURCEX_CLIENT)
public interface ITable {

    /**
     * 执行sql查询
     *
     * @param source 数据源信息
     * @param sql    查询sql
     * @return 查询结果
     */
    List<Map<String, Object>> executeQuery(@RpcNodeSign("tenantId") ISourceDTO source, String sql);

    /**
     * 执行sql，不需要结果
     *
     * @param source 数据源信息
     * @param sql    查询sql
     * @return 执行成功与否
     */
    Boolean executeSqlWithoutResultSet(@RpcNodeSign("tenantId") ISourceDTO source, String sql);

    /**
     * 获取所有分区，格式同sql返回值，不错额外处理，如：pt1=name1/pt2=name2/pt3=name3
     * 非分区表返回null
     *
     * @param source    数据源信息
     * @param tableName 表名
     * @return 所有分区
     */
    List<String> showPartitions(@RpcNodeSign("tenantId") ISourceDTO source, String tableName);

    /**
     * 删除表，成功返回true，失败返回false
     *
     * @param source    数据源信息
     * @param tableName 表名
     * @return 删除结果
     */
    Boolean dropTable(@RpcNodeSign("tenantId") ISourceDTO source, String tableName);

    /**
     * 重命名表，成功返回true，失败返回false
     *
     * @param source       数据源信息
     * @param oldTableName 旧表名
     * @param newTableName 新表名
     * @return 重命名结果
     */
    Boolean renameTable(@RpcNodeSign("tenantId") ISourceDTO source, String oldTableName, String newTableName);

    /**
     * 修改表参数
     *
     * @param source    数据源信息
     * @param tableName 表名
     * @param params    修改的参数，map集合
     * @return 修改结果
     */
    Boolean alterTableParams(@RpcNodeSign("tenantId") ISourceDTO source, String tableName, Map<String, String> params);

    /**
     * 获取表占用存储
     *
     * @param source    数据源信息
     * @param schema    schema信息
     * @param tableName 表名
     * @return 占用存储
     */
    Long getTableSize(@RpcNodeSign("tenantId") ISourceDTO source, String schema, String tableName);

    /**
     * 判断表是否是分区表
     *
     * @param source    数据源信息
     * @param schema    schema名称
     * @param tableName 表名
     * @return 是否是分区表
     */
    @Deprecated
    Boolean isView(@RpcNodeSign("tenantId") ISourceDTO source, String schema, String tableName);


    /**
     * 新增或者修改表的列
     *
     * @param source        数据源信息
     * @param columnMetaDTO 列信息
     * @return 新增或修改是否成功
     */
    Boolean upsertTableColumn(@RpcNodeSign("tenantId") ISourceDTO source, UpsertColumnMetaDTO columnMetaDTO);

    /**
     * 监听数据库bin log字段变更信息
     * @param source 数据源信息
     * @param binlogConfigDTO bin log监听配置
     * @param consumer 回调处理逻辑
     * @return 字段变更信息
     */
    Boolean monitorBinlog(@RpcNodeSign("tenantId") ISourceDTO source, BinlogConfigDTO binlogConfigDTO, Consumer<BinlogCallBackLogDTO> consumer);
}
