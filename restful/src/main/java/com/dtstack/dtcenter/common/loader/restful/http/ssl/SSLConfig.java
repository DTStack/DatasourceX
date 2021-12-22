package com.dtstack.dtcenter.common.loader.restful.http.ssl;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.Optional;

public class SSLConfig {
    public static final String KEYTYPE = "ssl.client.keystore.type";
    public static final String KEYLOCATION = "ssl.client.keystore.location";
    public static final String KEYPASSWORD = "ssl.client.keystore.password";
    public static final String TRUSTTYPE = "ssl.client.truststore.type";
    public static final String TRUSTLOCATION = "ssl.client.truststore.location";
    public static final String TRUSTPASSWORD = "ssl.client.truststore.password";
    public static final String TRUSTINTERVAL = "ssl.client.truststore.reload.interval";

    @JsonAlias(KEYTYPE)
    private String sslKeyStoreType;

    @JsonAlias(KEYLOCATION)
    private String sslKeyStorePath;

    @JsonAlias(KEYPASSWORD)
    private String sslKeyStorePassword;

    @JsonAlias(TRUSTTYPE)
    private String sslTrustStoreType;

    @JsonAlias(TRUSTLOCATION)
    private String sslTrustStorePath;

    @JsonAlias(TRUSTPASSWORD)
    private String sslTrustStorePassword;

    @JsonAlias(TRUSTINTERVAL)
    private String sslTrustStoreReloadInterval;

    public SSLConfig() {
    }

    public SSLConfig(String sslKeyStoreType, String sslKeyStorePath, String sslKeyStorePassword, String sslTrustStoreType, String sslTrustStorePath, String sslTrustStorePassword, String sslTrustStoreReloadInterval) {
        this.sslKeyStoreType = sslKeyStoreType;
        this.sslKeyStorePath = sslKeyStorePath;
        this.sslKeyStorePassword = sslKeyStorePassword;
        this.sslTrustStoreType = sslTrustStoreType;
        this.sslTrustStorePath = sslTrustStorePath;
        this.sslTrustStorePassword = sslTrustStorePassword;
        this.sslTrustStoreReloadInterval = sslTrustStoreReloadInterval;
    }

    public String getSslKeyStoreType() {
        return sslKeyStoreType;
    }

    public void setSslKeyStoreType(String sslKeyStoreType) {
        this.sslKeyStoreType = sslKeyStoreType;
    }

    public String getSslKeyStorePath() {
        return sslKeyStorePath;
    }

    public void setSslKeyStorePath(String sslKeyStorePath) {
        this.sslKeyStorePath = sslKeyStorePath;
    }

    public String getSslKeyStorePassword() {
        return sslKeyStorePassword;
    }

    public void setSslKeyStorePassword(String sslKeyStorePassword) {
        this.sslKeyStorePassword = sslKeyStorePassword;
    }

    public Optional<String> getSslTrustStoreType() {
        return Optional.ofNullable(sslTrustStoreType);
    }

    public void setSslTrustStoreType(String sslTrustStoreType) {
        this.sslTrustStoreType = sslTrustStoreType;
    }

    public String getSslTrustStorePath() {
        return sslTrustStorePath;
    }

    public void setSslTrustStorePath(String sslTrustStorePath) {
        this.sslTrustStorePath = sslTrustStorePath;
    }

    public Optional<String> getSslTrustStorePassword() {
        return Optional.ofNullable(sslTrustStorePassword);
    }

    public void setSslTrustStorePassword(String sslTrustStorePassword) {
        this.sslTrustStorePassword = sslTrustStorePassword;
    }

    public String getSslTrustStoreReloadInterval() {
        return sslTrustStoreReloadInterval;
    }

    public void setSslTrustStoreReloadInterval(String sslTrustStoreReloadInterval) {
        this.sslTrustStoreReloadInterval = sslTrustStoreReloadInterval;
    }
}
