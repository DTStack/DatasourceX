package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import com.dtstack.dtcenter.loader.enums.RedisMode;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:33 2020/1/17
 * @Description：数据库连接 传输对象
 */
public class SourceDTO {

    /**
     * 数据源类型
     */
    private DataSourceType sourceType;

    /**
     * URL 地址，如果为 master slave 的则为所有的地址
     */
    private String url;

    /**
     * 如果为 master slave 的则为 master 的地址
     */
    private String master;

    /**
     * 端口号
     */
    private String hostPort;

    private String schema;

    private String username;

    private String password;

    private RedisMode redisMode;

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

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
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

    public RedisMode getRedisMode() {
        return redisMode;
    }

    public void setRedisMode(RedisMode redisMode) {
        this.redisMode = redisMode;
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

        public SourceDTOBuilder setMaster(String master) {
            source.setMaster(master);
            return this;
        }

        public SourceDTOBuilder setHostPort(String hostPort) {
            source.setHostPort(hostPort);
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

        public SourceDTOBuilder setRedisMode(RedisMode redisMode) {
            source.setRedisMode(redisMode);
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
