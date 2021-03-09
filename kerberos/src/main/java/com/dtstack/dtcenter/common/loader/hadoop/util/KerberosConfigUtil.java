package com.dtstack.dtcenter.common.loader.hadoop.util;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.PathUtils;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:36 2020/8/27
 * @Description：Kerberos 配置工具
 */
@Slf4j
public class KerberosConfigUtil {
    /**
     * 校验 KEYTAB 文件，并在 confMap 中
     *
     * @param fileList
     * @param oppositeLocation
     * @param confMap
     */
    public static void dealKeytab(List<File> fileList, String oppositeLocation, Map<String, Object> confMap) {
        String finalPath = dealFilePath(fileList, oppositeLocation, DtClassConsistent.PublicConsistent.KEYTAB_SUFFIX);
        log.info("处理 Keytab 路径地址为绝对路径 -- key : {}, value : {}", HadoopConfTool.PRINCIPAL_FILE, finalPath);
        confMap.put(HadoopConfTool.PRINCIPAL_FILE, finalPath);
    }

    /**
     * 处理 krb5.conf 文件
     *
     * @param fileList
     * @param oppositeLocation
     * @param confMap
     */
    public static void dealKrb5Conf(List<File> fileList, String oppositeLocation, Map<String, Object> confMap) {
        String finalPath = dealFilePath(fileList, oppositeLocation, DtClassConsistent.PublicConsistent.KRB5CONF_FILE);
        log.info("处理 Krb5 路径地址为绝对路径 -- key : {}, value : {}", HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, finalPath);
        confMap.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, finalPath);
    }

    /**
     * 处理路径地址为绝对路径
     *
     * @param fileList
     * @param oppositeLocation
     * @param fileNameEnd
     * @return
     * @throws IOException
     */
    public static String dealFilePath(List<File> fileList, String oppositeLocation, String fileNameEnd) {
        Optional<File> krb5confOptional = fileList.stream().filter(file ->
                file.getName().endsWith(fileNameEnd)).findFirst();

        if (!krb5confOptional.isPresent()) {
            throw new DtLoaderException(String.format("以%s结尾的文件不存在", fileNameEnd));
        }

        String canonicalPath;
        try {
            canonicalPath = krb5confOptional.get().getCanonicalPath();
        } catch (IOException e) {
            throw new DtLoaderException(String.format("krb5文件无效 : %s", e.getMessage()), e);
        }
        return StringUtils.replace(canonicalPath, oppositeLocation, "");
    }

    /**
     * 改动相对路径为绝对路径
     *
     * @param conf
     * @param localKerberosPath
     * @param checkKey
     */
    public static void changeRelativePathToAbsolutePath(Map<String, Object> conf, String localKerberosPath, String checkKey) {
        String relativePath = MapUtils.getString(conf, checkKey);
        if (StringUtils.isBlank(relativePath)) {
            return;
        }

        // 如果目录超过三级，说明不是相对路径，已经被改为绝对路径了
        if (relativePath.split("/").length > 3) {
            return;
        }

        String absolutePath = PathUtils.removeMultiSeparatorChar(localKerberosPath + File.separator + relativePath);
        log.info("changeRelativePathToAbsolutePath checkKey:{} relativePath:{}, localKerberosConfPath:{}, absolutePath:{}", checkKey, relativePath, localKerberosPath, absolutePath);
        conf.put(checkKey, absolutePath);
    }

    /**
     * 从 Keytab 中获取 Principal 信息
     *
     * @param keytabPath
     * @return
     */
    public static List<String> getPrincipals(String keytabPath) {
        File file = new File(keytabPath);
        Keytab keytab = null;

        try {
            keytab = Keytab.loadKeytab(file);
        } catch (IOException e) {
            throw new DtLoaderException("解析 keytab 文件失败", e);
        }

        if (CollectionUtils.isEmpty(keytab.getPrincipals())) {
            throw new DtLoaderException("keytab 中的 Principal 为空");
        }

        return keytab.getPrincipals().stream().map(PrincipalName::getName).collect(Collectors.toList());
    }

    /**
     * 将 Map 转换为 Configuration
     *
     * @param configMap
     * @return
     */
    public static Configuration getConfig(Map<String, Object> configMap) {
        Configuration conf = new Configuration(false);
        Iterator var2 = configMap.entrySet().iterator();

        while (var2.hasNext()) {
            Map.Entry<String, Object> entry = (Map.Entry) var2.next();
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                conf.set(entry.getKey(), entry.getValue().toString());
            }
        }

        return conf;
    }

    /**
     * 从 JDBC_URL 中获取 Principal 信息
     *
     * @param url
     * @return
     */
    public static String getPrincipalFromUrl(String url) {
        if (StringUtils.isBlank(url)) {
            throw new DtLoaderException("jdbcUrl 信息为空");
        }

        log.info("get url principal : {}", url);
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
        throw new DtLoaderException("jdbcUrl 中不包含 Principal 信息 : " + url);
    }
}
