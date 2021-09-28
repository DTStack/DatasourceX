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

package com.dtstack.dtcenter.loader.client.issue;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

import java.io.IOException;

/**
 * bug描述：在kerberos zip 配置文件中，keytab和krb5文件不应该存在于目录中，如果存在，应当报错
 *
 * bug链接：http://zenpms.dtstack.cn/zentao/bug-view-36310.html
 *
 * bug解决：对解析后的相对文件路径多一层判断
 *
 * @author ：wangchuan
 * date：Created in 下午3:13 2021/4/14
 * company: www.dtstack.com
 */
public class Issue36310Test extends BaseTest {

    @Test(expected = DtLoaderException.class)
    public void test_for_issue() throws IOException {
        String zipPath = Issue36310Test.class.getResource("/bug/issue_36310/dir_kerberos.zip").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.HIVE.getVal());
        kerberos.parseKerberosFromUpload(zipPath, Issue36310Test.class.getResource("/").getPath());
    }
}
