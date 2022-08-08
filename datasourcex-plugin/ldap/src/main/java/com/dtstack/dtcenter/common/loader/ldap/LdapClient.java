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

package com.dtstack.dtcenter.common.loader.ldap;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.LdapSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import lombok.extern.slf4j.Slf4j;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

/**
 * ldap client
 *
 * @author luming
 * @date 2022/4/25
 */
@Slf4j
public class LdapClient extends AbsNoSqlClient {

    @Override
    public Boolean testCon(ISourceDTO source) {
        LdapSourceDTO sourceDTO = (LdapSourceDTO) source;
        Hashtable<String, String> env = new Hashtable<>();
        DirContext dct = null;

        AssertUtils.notBlank(sourceDTO.getUrl(), "ldap url can't be null");
        AssertUtils.notBlank(sourceDTO.getUsername(), "ldap account can't be null");
        AssertUtils.notBlank(sourceDTO.getPassword(), "ldap passwd can't be null");

        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, sourceDTO.getUrl());
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, sourceDTO.getUsername());
        env.put(Context.SECURITY_CREDENTIALS, sourceDTO.getPassword());

        try {
            //初始化LDAP连接，连接成功后就可以用dct来操作LDAP了
            dct = new InitialDirContext(env);
        } catch (NamingException e) {
            throw new DtLoaderException("ldap connect error.", e);
        } finally {
            try {
                assert dct != null;
                dct.close();
            } catch (NamingException e) {
                log.error("ldap close error.", e);
            }
        }

        return true;
    }
}
