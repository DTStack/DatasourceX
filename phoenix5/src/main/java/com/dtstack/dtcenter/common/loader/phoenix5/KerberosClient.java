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

package com.dtstack.dtcenter.common.loader.phoenix5;

import com.dtstack.dtcenter.common.loader.hadoop.AbsKerberosClient;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * phoenix5 kerberos客户端
 *
 * @author ：wangchuan
 * date：Created in 下午 05:10 2021/1/11
 * company: www.dtstack.com
 */
@Slf4j
public class KerberosClient extends AbsKerberosClient {
    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        super.prepareKerberosForConnect(conf, localKerberosPath);
        // phoenix 和 hbase一样 需要两张 Principal 所以这边做一下处理
        if (!conf.containsKey(HadoopConfTool.PRINCIPAL)) {
            String principal = getPrincipals(conf).get(0);
            log.info("setting principal 为 {}", principal);
            conf.put(HadoopConfTool.PRINCIPAL, principal);
        }
        if (!conf.containsKey(HadoopConfTool.HBASE_MASTER_PRINCIPAL)) {
            throw new DtLoaderException(String.format("phoenix   must setting %s ", HadoopConfTool.HBASE_MASTER_PRINCIPAL));
        }

        if (!conf.containsKey(HadoopConfTool.HBASE_REGION_PRINCIPAL)) {
            log.info("setting hbase.regionserver.kerberos.principal 为 {}", conf.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
            conf.put(HadoopConfTool.HBASE_REGION_PRINCIPAL, conf.get(HadoopConfTool.HBASE_MASTER_PRINCIPAL));
        }

        return true;
    }
}
