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

package com.dtstack.dtcenter.common.loader.hadoop;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.Xml2JsonUtil;
import com.dtstack.dtcenter.common.loader.common.utils.ZipUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosUtil;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.source.AbstractSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.utils.AssertUtils;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * kerberos 抽象客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:38 2021/8/11
 * company: www.dtstack.com
 */
@Slf4j
public class AbsKerberosClient implements IKerberos {

    @Override
    public Map<String, Object> parseKerberosFromUpload(ISourceDTO sourceDTO, String zipLocation, String localKerberosPath) {
        // 返回的配置信息
        Map<String, Object> confMap = new HashMap<>();

        // 解压缩文件并过滤隐藏文件(点开始的)
        List<File> unzipFileList = ZipUtil.unzipFile(zipLocation, localKerberosPath);
        unzipFileList = unzipFileList.stream().filter(file -> !file.getName().startsWith(".")).collect(Collectors.toList());

        // 处理 Krb 和 Keytab 信息
        dealFile(unzipFileList, localKerberosPath, confMap);

        // 获取 XML 文件并解析为 MAP
        List<File> xmlFileList = unzipFileList.stream().filter(file -> file.getName().endsWith(DtClassConsistent.PublicConsistent.XML_SUFFIX))
                .collect(Collectors.toList());
        xmlFileList.forEach(file -> confMap.putAll(Xml2JsonUtil.xml2map(file)));

        return confMap;
    }

    /**
     * 处理文件信息
     *
     * @param unzipFileList     解压后的所有文件
     * @param localKerberosPath 本地文件夹路径
     * @param confMap           kerberos 配置
     */
    protected void dealFile(List<File> unzipFileList, String localKerberosPath, Map<String, Object> confMap) {
        KerberosConfigUtil.dealKeytab(unzipFileList, localKerberosPath, confMap);
        KerberosConfigUtil.dealKrb5Conf(unzipFileList, localKerberosPath, confMap);
    }

    @Override
    public String getPrincipals(ISourceDTO sourceDTO, String url) {
        return KerberosConfigUtil.getPrincipalFromUrl(url);
    }

    @Override
    public List<String> getPrincipals(ISourceDTO sourceDTO, Map<String, Object> kerberosConfig) {
        AbstractSourceDTO abstractSourceDTO = (AbstractSourceDTO) sourceDTO;
        // 兼容 sourceDTO 中传 kerberos 的情况
        KerberosUtil.downloadAndReplace(MapUtils.isEmpty(kerberosConfig) ? abstractSourceDTO.getKerberosConfig() : kerberosConfig);
        String principalFile = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        if (StringUtils.isBlank(principalFile)) {
            throw new DtLoaderException("get Principal message，Keytab setting not exits");
        }
        return KerberosConfigUtil.getPrincipals(principalFile);
    }

    @Override
    public Map<String, Object> parseKerberosFromLocalDir(ISourceDTO sourceDTO, String kerberosDir) {
        AssertUtils.notBlank(kerberosDir, "local kerberos dir can't be null.");
        File kerberosFile = new File(kerberosDir);
        AssertUtils.isTrue(kerberosFile.exists(), String.format("local kerberos dir: [%s] is not exists.", kerberosDir));
        AssertUtils.isTrue(kerberosFile.isDirectory(), String.format("local kerberos path: [%s] is not directory.", kerberosDir));
        File[] kerberosFiles = kerberosFile.listFiles();
        AssertUtils.isTrue(Objects.nonNull(kerberosFiles) && kerberosFiles.length > 0, String.format("the kerberos dir [%s] is empty.", kerberosDir));
        List<File> kerberosFileList = Arrays.stream(kerberosFiles).filter(file -> !file.getName().startsWith(".")).collect(Collectors.toList());

        String principalFilePath = kerberosFileList.stream()
                .filter(kerFile -> kerFile.getName().endsWith(".keytab"))
                .map(File::getAbsolutePath)
                .findAny()
                .orElseThrow(() -> new DtLoaderException(String.format("the kerberos dir [%s] does not contain files ending in .keytab", kerberosDir)));

        String krb5ConfPath = kerberosFileList.stream()
                .filter(kerFile -> kerFile.getName().equals("krb5.conf"))
                .map(File::getAbsolutePath)
                .findAny()
                .orElseThrow(() -> new DtLoaderException(String.format("the kerberos dir [%s] does not contain files equal krb5.conf", kerberosDir)));

        Map<String, Object> confMap = Maps.newHashMap();
        confMap.put(HadoopConfTool.PRINCIPAL_FILE, principalFilePath);
        confMap.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, krb5ConfPath);

        // 获取 XML 文件并解析为 MAP
        List<File> xmlFileList = kerberosFileList.stream().filter(file -> file.getName().endsWith(DtClassConsistent.PublicConsistent.XML_SUFFIX))
                .collect(Collectors.toList());
        xmlFileList.forEach(file -> confMap.putAll(Xml2JsonUtil.xml2map(file)));
        return confMap;
    }

    @Override
    public Boolean authTest(ISourceDTO sourceDTO, Map<String, Object> kerberosConfig) {
        UserGroupInformation ugi = KerberosLoginUtil.loginWithUGI(kerberosConfig);
        AssertUtils.notNull(ugi, "ugi can't be null");
        return true;
    }
}
