package com.dtstack.dtcenter.common.loader.hadoop.util;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.PathUtils;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public static void dealKeytab(List<File> fileList, String oppositeLocation, Map<String, String> confMap) throws IOException {
        // 校验 keytab 文件是否存在
        Optional<File> keytabOptional = fileList.stream().filter(file
                -> file.getName().endsWith(DtClassConsistent.PublicConsistent.KEYTAB_SUFFIX)).findFirst();
        if (!keytabOptional.isPresent()) {
            throw new DtLoaderException("keytab 文件不存在");
        }

        String canonicalPath = keytabOptional.get().getCanonicalPath();
        confMap.put(HadoopConfTool.PRINCIPAL_FILE, StringUtils.replace(canonicalPath, oppositeLocation, ""));
    }

    /**
     * 处理 krb5.conf 文件
     *
     * @param fileList
     * @param oppositeLocation
     * @param confMap
     */
    public static void dealKrb5Conf(List<File> fileList, String oppositeLocation, Map<String, String> confMap) throws IOException {
        Optional<File> krb5confOptional = fileList.stream().filter(file ->
                DtClassConsistent.PublicConsistent.KRB5CONF_FILE.equals(file.getName())).findFirst();

        if (!krb5confOptional.isPresent()) {
            return;
        }

        String canonicalPath = krb5confOptional.get().getCanonicalPath();
        confMap.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, StringUtils.replace(canonicalPath, oppositeLocation, ""));
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
        if (StringUtils.isNotBlank(relativePath)) {
            String absolutePath = PathUtils.removeMultiSeparatorChar(localKerberosPath + File.separator + relativePath);
            log.info("changeRelativePathToAbsolutePath checkKey:{} relativePath:{}, localKerberosConfPath:{}, absolutePath:{}", checkKey, relativePath, localKerberosPath, absolutePath);
            conf.put(checkKey, absolutePath);
        }
    }
}
