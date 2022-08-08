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

package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * ldap数据源信息
 *
 * @author luming
 * @date 2022/4/25
 */
@EqualsAndHashCode(callSuper = true)
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class LdapSourceDTO extends AbstractSourceDTO {
    /**
     * LDAP的账户名，一般是这样的格式：cn=xx,dc=xx,dc=com ，根据LDAP的配置情况来
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * LDAP的地址，389是LDAP的默认端口
     * eg:
     * ldap://domain:389
     */
    private String url;

    /**
     * 数据源类型
     */
    protected Integer sourceType;

    @Override
    public Integer getSourceType() {
        return DataSourceType.LDAP.getVal();
    }
}
