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

package com.dtstack.dtcenter.loader.client.common;

import com.dtstack.dtcenter.common.loader.clickhouse.ClickhouseAdapter;
import com.dtstack.dtcenter.common.loader.kingbase.KingbaseAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class AdapterTest {

    @Test
    public void test() throws ClassNotFoundException, IllegalAccessException {
        //获取所有变量的值
        Class clazz = Class.forName("java.sql.Types");
        Field[] fields = clazz.getFields();
        Assert.assertEquals(39, fields.length);
        for( Field field : fields ){
            ClickhouseAdapter.mapColumnTypeJdbc2Java(field.getInt(clazz),1,1);
            KingbaseAdapter.mapColumnTypeJdbc2Java(field.getInt(clazz),1,1);
        }
        ClickhouseAdapter.mapColumnTypeJdbc2Oracle(1,1,1);
        KingbaseAdapter.mapColumnTypeJdbc2Oracle(1,1,1);
    }

}
