package com.dtstack.dtcenter.common.loader.odps.pool;

import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午3:19 2020/8/3
 * @Description：odps 连接池配置类
 */
@Data
public class OdpsPoolConfig extends GenericObjectPoolConfig {

    private String odpsServer;

    private String accessId;

    private String accessKey;

    private String project;

    private String packageAuthorizedProject;

    private String accountType;
}
