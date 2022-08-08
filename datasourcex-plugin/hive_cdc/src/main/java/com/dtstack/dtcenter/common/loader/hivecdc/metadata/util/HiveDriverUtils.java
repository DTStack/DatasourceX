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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.util;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.security.PrivilegedAction;
import java.util.Map;

import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.ORC_FORMAT;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.PARQUET_FORMAT;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.TEXT_FORMAT;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.TYPE_ORC;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.TYPE_PARQUET;
import static com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons.HiveMetaCons.TYPE_TEXT;

/**
 * hive相关utils
 *
 * @author luming
 * @date 2022/4/18
 */
@Slf4j
public class HiveDriverUtils {
    /**
     * metaStore 地址 key
     */
    private final static String META_STORE_URIS_KEY = "hive.metastore.uris";
    /**
     * 是否启用 kerberos 认证
     */
    private final static String META_STORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    /**
     * metaStore 地址 principal 地址
     */
    private final static String META_STORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";

    /**
     * 检查 metaStore 连通性
     *
     * @param metaStoreUris  metaStore 地址
     * @param kerberosConfig kerberos 配置
     */
    public static HiveMetaStoreClient getMetaStoreClient(String metaStoreUris, Map<String, Object> kerberosConfig) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(META_STORE_URIS_KEY, metaStoreUris);

        KerberosUtil.downloadAndReplace(kerberosConfig);

        // 重新设置 metaStore 地址
        if (MapUtils.isNotEmpty(kerberosConfig)) {
            // metaStore kerberos 认证需要
            hiveConf.setBoolean(META_STORE_SASL_ENABLED, true);
            // 做两步兼容：先取 hive.metastore.kerberos.principal 的值，再取 principal，最后再取 keytab 中的第一个 principal
            String metaStorePrincipal = MapUtils.getString(kerberosConfig, META_STORE_KERBEROS_PRINCIPAL, MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL));
            if (StringUtils.isBlank(metaStorePrincipal)) {
                String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
                metaStorePrincipal = KerberosConfigUtil.getPrincipals(keytabPath).get(0);
                if (StringUtils.isBlank(metaStorePrincipal)) {
                    throw new DtLoaderException("hive.metastore.kerberos.principal is not null...");
                }
            }
            log.info("hive.metastore.kerberos.principal:{}", metaStorePrincipal);
            hiveConf.set(META_STORE_KERBEROS_PRINCIPAL, metaStorePrincipal);
        }
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<HiveMetaStoreClient>) () -> {
                    try {
                        return new HiveMetaStoreClient(hiveConf);
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("metastore connection failed.:%s", e.getMessage()));
                    }
                }
        );
    }

    /**
     * 获取hive store的方式
     *
     * @param storedClass str
     * @return stored type
     */
    public static String getStoredType(String storedClass) {
        if (storedClass.endsWith(TEXT_FORMAT)) {
            return TYPE_TEXT;
        } else if (storedClass.endsWith(ORC_FORMAT)) {
            return TYPE_ORC;
        } else if (storedClass.endsWith(PARQUET_FORMAT)) {
            return TYPE_PARQUET;
        } else {
            return storedClass;
        }
    }
}
