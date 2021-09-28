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

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.EMQSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Date: 2020/4/9
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
@Ignore
public class EMQTest extends BaseTest {
    IClient client = ClientCache.getClient(DataSourceType.EMQ.getVal());

    // 没有可用的数据源信息
    EMQSourceDTO source = EMQSourceDTO.builder()
            .url("tcp://kudu5:1883")
            .build();

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test(expected = DtLoaderException.class)
    public void getCon() throws Exception {
        client.getCon(source);
    }

    @Test(expected = DtLoaderException.class)
    public void executeQuery() throws Exception {
        client.executeQuery(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void executeSqlWithoutResultSet() throws Exception {
        client.executeSqlWithoutResultSet(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTableList() throws Exception {
        client.getTableList(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() throws Exception {
        client.getColumnClassInfo(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaData() throws Exception {
        client.getColumnMetaData(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() throws Exception {
        client.getColumnMetaDataWithSql(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() throws Exception {
        client.getFlinkColumnMetaData(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() throws Exception {
        client.getTableMetaComment(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getPreview() throws Exception {
        client.getPreview(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() throws Exception {
        client.getDownloader(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getAllDatabases() throws Exception {
        client.getAllDatabases(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() throws Exception {
        client.getCreateTableSql(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() throws Exception {
        client.getPartitionColumn(source, null);
    }
}
