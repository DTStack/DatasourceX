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

package com.dtstack.dtcenter.common.loader.oracle;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:50 2020/8/20
 * @Description：Oracle 字段转化测试
 */
public class OracleDaAdapterTest {

    @Test
    public void mapColumnTypeJdbc2Java() {
        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_VARCHAR.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.CHAR, 0, 0));
        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_VARCHAR.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.NCLOB, 0, 0));

        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_TIMESTAMP.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.TIME, 0, 0));
        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_TIMESTAMP.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.TIMESTAMP, 0, 0));
        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_TIMESTAMP.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.TIMESTAMP, 0, 1));

        Assert.assertEquals(OracleDbAdapter.JavaType.TYPE_INT.getFlinkSqlType(), OracleDbAdapter.mapColumnTypeJdbc2Java(Types.TINYINT, 0, 0));
    }
}
