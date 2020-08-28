package com.dtstack.dtcenter.common.loader.hadoop;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.Xml2JsonUtil;
import com.dtstack.dtcenter.common.loader.common.ZipUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosConfigUtil;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:32 2020/8/26
 * @Description：Kerberos 服务客户端
 */
public class AbsKerberosClient implements IKerberos {

    @Override
    public Map<String, String> parseKerberosFromUpload(String zipLocation, String localKerberosPath, Integer datasourceType) throws Exception {
        // 返回的配置信息
        Map<String, String> confMap = new HashMap<>();

        // 解压缩文件并过滤隐藏文件(点开始的)
        List<File> unzipFileList = ZipUtil.unzipFile(zipLocation, localKerberosPath);
        unzipFileList = unzipFileList.stream().filter(file -> file.getName().startsWith(".")).collect(Collectors.toList());

        // 处理 KEYTAB
        KerberosConfigUtil.dealKeytab(unzipFileList, localKerberosPath, confMap);
        KerberosConfigUtil.dealKrb5Conf(unzipFileList, localKerberosPath, confMap);

        // 获取 XML 文件并解析为 MAP
        List<File> xmlFileList = unzipFileList.stream().filter(file -> file.getName().endsWith(DtClassConsistent.PublicConsistent.XML_SUFFIX))
                .collect(Collectors.toList());
        xmlFileList.forEach(file -> confMap.putAll(Xml2JsonUtil.xml2map(file)));

        return confMap;
    }

    @Override
    public Boolean prepareKerberosForConnect(Map<String, Object> conf, String localKerberosPath, Integer datasourceType) throws Exception {
        // 替换相对路径
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KAFKA_KERBEROS_KEYTAB);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.PRINCIPAL_FILE);
        KerberosConfigUtil.changeRelativePathToAbsolutePath(conf, localKerberosPath, HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF);
        return true;
    }

    @Override
    public String getPrincipal(String url, Integer datasourceType) throws Exception {
        Matcher matcher = DtClassConsistent.PatternConsistent.JDBC_PATTERN.matcher(url);
        if (matcher.find()) {
            String params = matcher.group("param");
            String[] split = params.split(";");
            for (String param : split) {
                String[] keyValue = param.split("=");
                if (HadoopConfTool.PRINCIPAL.equals(keyValue[0])) {
                    return keyValue.length > 1 ? keyValue[1] : StringUtils.EMPTY;
                }
            }
        }
        return StringUtils.EMPTY;
    }

    @Override
    public List<String> getPrincipal(Map<String, Object> kerberosConfig, Integer datasourceType) throws Exception {
        return null;
    }
}
