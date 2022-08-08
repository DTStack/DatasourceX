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

package com.dtstack.dtcenter.loader.cache.client.kerberos;

import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.callback.ClassLoaderCallBackMethod;

import java.util.List;
import java.util.Map;

/**
 * kerberos 代理类
 *
 * @author ：nanqi
 * date：Created in 下午8:19 2022/2/23
 * company: www.dtstack.com
 */
public class KerberosProxy implements IKerberos {

    IKerberos targetClient;

    public KerberosProxy(IKerberos kerberos) {
        this.targetClient = kerberos;
    }

    @Override
    public Map<String, Object> parseKerberosFromUpload(ISourceDTO sourceDTO, String zipLocation, String localKerberosPath) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.parseKerberosFromUpload(sourceDTO, zipLocation, localKerberosPath),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public String getPrincipals(ISourceDTO sourceDTO, String url) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(sourceDTO, url),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public List<String> getPrincipals(ISourceDTO sourceDTO, Map<String, Object> kerberosConfig) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPrincipals(sourceDTO, kerberosConfig),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Map<String, Object> parseKerberosFromLocalDir(ISourceDTO sourceDTO, String kerberosDir) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.parseKerberosFromLocalDir(sourceDTO, kerberosDir),
                targetClient.getClass().getClassLoader());
    }

    @Override
    public Boolean authTest(ISourceDTO sourceDTO, Map<String, Object> kerberosConfig) {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.authTest(sourceDTO, kerberosConfig),
                targetClient.getClass().getClassLoader());
    }
}
