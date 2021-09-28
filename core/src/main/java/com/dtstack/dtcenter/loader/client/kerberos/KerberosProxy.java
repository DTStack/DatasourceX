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

package com.dtstack.dtcenter.loader.client.kerberos;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKerberos;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:42 2020/8/26
 * @Description：Kerberos 实现类
 */
public class KerberosProxy implements IKerberos {
    IKerberos targetClient = null;

    public KerberosProxy(IKerberos kerberos) {
        this.targetClient = kerberos;
    }

    @Override
    public Map<String, Object> parseKerberosFromUpload(String zipLocation, String localKerberosPath) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.parseKerberosFromUpload(zipLocation, localKerberosPath),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.prepareKerberosForConnect(conf, localKerberosPath),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getPrincipals(String url) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(url),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getPrincipals(Map<String, Object> kerberosConfig) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(kerberosConfig),
                targetClient.getClass().getClassLoader());
    }
}
