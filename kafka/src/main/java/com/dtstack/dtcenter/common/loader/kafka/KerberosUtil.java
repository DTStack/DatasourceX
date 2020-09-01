package com.dtstack.dtcenter.common.loader.kafka;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:09 2020/8/31
 * @Description：Kerberos 工具类
 */
@Slf4j
public class KerberosUtil {

    /**
     * 根据 Keytab 获取 Principal
     * @param keytabPath
     * @return
     */
    public static String getPrincipal(String keytabPath) {
        File file = new File(keytabPath);
        Preconditions.checkState(file.exists() && file.isFile(), String.format("can't read keytab file (%s),it's  not exist or not a file", keytabPath));
        Keytab keytab = null;

        try {
            keytab = Keytab.loadKeytab(file);
        } catch (IOException var5) {
            log.error("Keytab loadKeytab error {}", var5);
            throw new DtCenterDefException("解析keytab文件失败");
        }

        List<PrincipalName> names = keytab.getPrincipals();
        if (CollectionUtils.isNotEmpty(names)) {
            if (log.isDebugEnabled()) {
                log.debug("all principal from keytab={}:", file.getName());
                names.stream().forEach((name) -> {
                    log.debug("principal={}", name.toString());
                });
            }

            PrincipalName principalName = (PrincipalName)names.get(0);
            if (Objects.nonNull(principalName)) {
                return principalName.getName();
            }
        }

        throw new DtCenterDefException("当前keytab文件不包含principal信息");
    }
}
