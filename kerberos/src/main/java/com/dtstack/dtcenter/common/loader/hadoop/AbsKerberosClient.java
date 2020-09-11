package com.dtstack.dtcenter.common.loader.hadoop;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.Xml2JsonUtil;
import com.dtstack.dtcenter.common.loader.common.ZipUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:32 2020/8/26
 * @Description：Kerberos 服务客户端
 */
@Slf4j
public class AbsKerberosClient implements IKerberos {

    @Override
    public Map<String, Object> parseKerberosFromUpload(String zipLocation, String localKerberosPath) throws Exception {
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
     * @param unzipFileList
     * @param localKerberosPath
     * @param confMap
     * @throws IOException
     */
    protected void dealFile(List<File> unzipFileList, String localKerberosPath, Map<String, Object> confMap) throws IOException {
        KerberosConfigUtil.dealKeytab(unzipFileList, localKerberosPath, confMap);
        KerberosConfigUtil.dealKrb5Conf(unzipFileList, localKerberosPath, confMap);
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath) throws Exception {
        // 替换相对路径
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.PRINCIPAL_FILE);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
        return true;
    }

    @Override
    public String getPrincipals(String url) throws Exception {
        return KerberosConfigUtil.getPrincipalFromUrl(url);
    }

    @Override
    public List<String> getPrincipals(Map<String, Object> kerberosConfig) throws Exception {
        String keytabPath = MapUtils.getString(kerberosConfig, HadoopConfTool.PRINCIPAL_FILE);
        if (StringUtils.isBlank(keytabPath)) {
            throw new DtLoaderException("获取 Principal 信息异常，Keytab 配置不存在");
        }
        return KerberosConfigUtil.getPrincipals(keytabPath);
    }
}
