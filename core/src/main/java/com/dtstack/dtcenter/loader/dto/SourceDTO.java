package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:33 2020/1/17
 * @Description：数据库连接 传输对象
 */
public class SourceDTO {

    private DataSourceType sourceType;

    private String url;

    private String schema;

    private String username;

    private String password;

    public DataSourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(DataSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public static class SourceDTOBuilder {
        private SourceDTO source = new SourceDTO();

        public SourceDTOBuilder setSourceType(DataSourceType sourceType) {
            source.setSourceType(sourceType);
            return this;
        }

        public SourceDTOBuilder setUrl(String url) {
            source.setUrl(url);
            return this;
        }

        public SourceDTOBuilder setSchema(String schema) {
            source.setSchema(schema);
            return this;
        }

        public SourceDTOBuilder setUsername(String username) {
            source.setUsername(username);
            return this;
        }

        public SourceDTOBuilder setPassword(String password) {
            source.setPassword(password);
            return this;
        }

        public SourceDTO builder() {
            return source;
        }
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        if (StringUtils.isNotBlank(getUsername())) {
            properties.setProperty(ConfigConstant.USER_NAME, getUsername());
        }
        if (StringUtils.isNotBlank(getPassword())) {
            properties.setProperty(ConfigConstant.PWD, getPassword());
        }
        return properties;
    }
}
