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

package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.UpsertColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.SqlserverSourceDTO;
import com.dtstack.dtcenter.loader.enums.CommandType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：LOADER_TEST
 * @Date ：Created in 04:10 2020/2/29
 * @Description：SQLServer 测试
 */
public class SQLServerTableTest extends BaseTest {
    // 获取数据源 client
    private static final ITable client = ClientCache.getTable(DataSourceType.SQLServer.getVal());

    private static final SqlserverSourceDTO source = SqlserverSourceDTO.builder()
            .url("jdbc:sqlserver://172.16.101.246:1433;databaseName=db_dev")
            .username("dev")
            .password("Abc12345")
            .poolConfig(PoolConfig.builder().build())
            .build();

    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.SQLServer.getVal());
        // 对表禁用CDC(变更数据捕获)功能
        String cdcCloseSql = "EXEC sys.sp_cdc_disable_table  " +
                "@source_schema = 'dev', " +
                "@source_name = 'LOADER_TEST', " +
                "@capture_instance = 'all' ";
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql(cdcCloseSql).build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // 不做处理
        }
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists LOADER_TEST").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, name nvarchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, '中文LOADER_TEST')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        // 添加表注释
        String commentSql = "exec sp_addextendedproperty N'MS_Description', N'中文_comment', N'SCHEMA', N'dev', N'TABLE', N'LOADER_TEST'";
        queryDTO = SqlQueryDTO.builder().sql(commentSql).build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        // 对表启用CDC(变更数据捕获)功能
        String cdcOpenSql = "EXEC sys.sp_cdc_enable_table " +
                        "@source_schema = 'dev', " +
                        "@source_name = 'LOADER_TEST', " +
                        "@role_name = NULL, " +
                        "@supports_net_changes = 0 ";
        queryDTO = SqlQueryDTO.builder().sql(cdcOpenSql).build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e) {
            // 不做处理
        }
    }

    /**
     * 获取连接测试
     */
    @Test
    public void getCon() throws Exception{
        Connection connection = client.getCon(source);
        Assert.assertNotNull(connection);
        connection.close();
    }

    @Test
    public void upsertTableColumn() {
        UpsertColumnMetaDTO columnMetaDTO = new UpsertColumnMetaDTO();
        columnMetaDTO.setCommandType(CommandType.INSERT);
        columnMetaDTO.setSchema("dev");
        columnMetaDTO.setTableName("LOADER_TEST");
        columnMetaDTO.setColumnComment("comment");
        columnMetaDTO.setColumnName("age");
        columnMetaDTO.setColumnType("int");
        client.upsertTableColumn(source, columnMetaDTO);
    }
}
