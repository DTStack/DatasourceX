package com.dtstack.dtcenter.common.loader.restful.http.ssl;

import com.dtstack.dtcenter.common.loader.common.utils.Xml2JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Map;

@Slf4j
public class SSLManager {

    public static SSLContext getSSLContext(SSLConfig sslConfig) {
        try {
            InputStream in = new FileInputStream(sslConfig.getSslTrustStorePath());
            KeyStore trustStore = KeyStore.getInstance(sslConfig.getSslTrustStoreType().orElse(KeyStore.getDefaultType()));
            try {
                trustStore.load(in, sslConfig.getSslTrustStorePassword().map(String::toCharArray).orElse(null));
            } finally {
                in.close();
            }
            SSLContext sslcontext = SSLContexts
                    .custom()
                    .loadTrustMaterial(trustStore, new TrustSelfSignedStrategy())  //加载本地信任证书
                    .build();//构造
            return sslcontext;
        } catch (Exception e) {
            log.info("get sslcontext fail", e);
            return null;
        }
    }

    public static SSLConfig getSSLConfig(String sslClientConf) {
        if (StringUtils.isNotBlank(sslClientConf)) {
            Map<String, String> sslMap = Xml2JsonUtil.xml2map(new File(sslClientConf));
            SSLConfig sslConfig = new SSLConfig();
            sslConfig.setSslTrustStorePath(String.valueOf(sslMap.get(SSLConfig.TRUSTLOCATION)));
            sslConfig.setSslTrustStorePassword(String.valueOf(sslMap.get(SSLConfig.TRUSTPASSWORD)));
            sslConfig.setSslTrustStoreType(String.valueOf(sslMap.get(SSLConfig.TRUSTTYPE)));
            sslConfig.setSslTrustStoreReloadInterval(String.valueOf(sslMap.get(SSLConfig.TRUSTINTERVAL)));
            return sslConfig;
        } else {
            return null;
        }
    }

    public static SSLContext getSSLContext(String sslClientConf) {
        SSLConfig sslConfig = SSLManager.getSSLConfig(sslClientConf);
        return sslConfig != null ? SSLManager.getSSLContext(sslConfig) : null;
    }

}
