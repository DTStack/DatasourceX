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

import com.dtstack.dtcenter.common.loader.impala.ImpalaClient;
import com.dtstack.dtcenter.common.loader.impala.ImpalaConnFactory;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;


public class ImpalaTest {


    @Test
    public void getCon() throws Exception {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .build();

        ImpalaConnFactory connFactory = new ImpalaConnFactory();
        connFactory.getConn(source, StringUtils.EMPTY);
    }

    @Test
    public void getTableList() throws Exception {
        ImpalaSourceDTO source = ImpalaSourceDTO.builder()
                .url("jdbc:impala://172.16.101.17:21050/default")
                .defaultFS("hdfs://ns1")
                .schema("aa_aa")
                .build();
        IClient client = new ImpalaClient();
        List<String> list = client.getTableList(source, SqlQueryDTO.builder().build());
        AssertUtils.notNull(list, "");
    }
}
