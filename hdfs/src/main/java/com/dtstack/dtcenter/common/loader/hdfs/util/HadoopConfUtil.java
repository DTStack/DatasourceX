package com.dtstack.dtcenter.common.loader.hdfs.util;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.hadoop.HadoopConfTool;
import com.dtstack.dtcenter.common.kerberos.KerberosConfigVerify;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;

/**
 * hadoop配置工具类
 *
 * @author ：wangchuan
 * date：Created in 上午11:51 2020/8/11
 * company: www.dtstack.com
 */

public class HadoopConfUtil {

    private static Logger logger = LoggerFactory.getLogger(HadoopConfUtil.class);

    private final static String HADOOP_CONFIGE = System.getProperty("user.dir") + "/conf/hadoop/";

    private final static String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");

    private static Configuration defaultConfiguration = new Configuration(false);

    private static final String FS_HDFS_IMPL_DISABLE_CACHE ="fs.hdfs.impl.disable.cache";

    private static final String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED ="ipc.client.fallback-to-simple-auth-allowed";


    static {
        try {
            String dir = StringUtils.isNotBlank(HADOOP_CONF_DIR) ? HADOOP_CONF_DIR : HADOOP_CONFIGE;
            defaultConfiguration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            defaultConfiguration.set("fs.hdfs.impl.disable.cache", "true");
            File[] xmlFileList = new File(dir).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.endsWith(".xml")) {
                        return true;
                    }
                    return false;
                }
            });

            if (xmlFileList != null) {
                for (File xmlFile : xmlFileList) {
                    defaultConfiguration.addResource(xmlFile.toURI().toURL());
                }
            }
        } catch (Exception e) {
            logger.error("{}", e);
        }
    }

    /**
     * 获取hdfs和yarn配置的总和
     *
     * @param hdfsConfig
     * @param yarnConfig
     * @return
     */
    public static Configuration getFullConfiguration(String hdfsConfig, Map<String, Object> yarnConfig) {
        Configuration hadoopConf = getHdfsConfiguration(hdfsConfig);
        YarnConfiguration yarnConfiguration = getYarnConfiguration(hdfsConfig, yarnConfig);

        for (Map.Entry<String, String> entry : yarnConfiguration) {
            hadoopConf.set(entry.getKey(), entry.getValue());
        }
        return hadoopConf;
    }

    /**
     * 获取hdfs配置
     * @param hdfsConfig
     * @return
     */
    public static Configuration getHdfsConfiguration(String hdfsConfig) {
        Configuration configuration = null;
        try {
            Map<String, Object> hdfsConf = JSONObject.parseObject(hdfsConfig);
            if (MapUtils.isNotEmpty(hdfsConf)) {
                configuration = initHadoopConf(hdfsConf);
            }
        } catch (Exception e) {
            logger.error("{}", e);
        }
        if (configuration == null) {
            configuration = defaultConfiguration;
        }
        return configuration;
    }


    /**
     * 构建yarn的配置信息
     * @param hdfsConfig hdfs配置信息 String类型
     * @param yarnConfig yarn配置信息 map类型
     * @return YarnConfiguration
     */
    public static YarnConfiguration getYarnConfiguration(String hdfsConfig, Map<String, Object> yarnConfig) {
        Configuration yarnConf;
        try{
            Configuration configuration = getHdfsConfiguration(hdfsConfig);
            yarnConf = new YarnConfiguration(configuration);
            initYarnConfiguration((YarnConfiguration)yarnConf, yarnConfig);
        }catch (Exception e){
            logger.error("{}", e);
            throw new DtLoaderException("获取yarn配置信息失败", e);
        }
        return (YarnConfiguration) yarnConf;
    }

    /**
     * 初始化hadoop配置
     * @param conf hadoop配置 map类型
     * @return
     */
    private static Configuration initHadoopConf(Map<String, Object> conf) {
        KerberosConfigVerify.replaceHost(conf);

        if (conf == null || conf.size() == 0) {
            //读取环境变量--走默认配置
            return defaultConfiguration;
        }

        Configuration configuration = new Configuration(false);
        setDefaultConf(configuration);

        for (String key : conf.keySet()) {
            configuration.set(key, conf.get(key).toString());
        }
        return configuration;
    }

    /**
     * 初始化yarn配置信息
     * @param yarnConfiguration
     * @param map
     */
    private static void initYarnConfiguration(YarnConfiguration yarnConfiguration, Map<String, Object> map){
        for(Map.Entry<String, Object> entry : map.entrySet()){
            yarnConfiguration.set(entry.getKey(), entry.getValue().toString());
        }
        setDefaultConf(yarnConfiguration);
    }

    public static String getDefaultFs(String hdfsConfig) {
        return getHdfsConfiguration(hdfsConfig).get("fs.defaultFS");
    }

    /**
     * 设置默认属性
     * @param conf
     */
    public static void setDefaultConf(Configuration conf) {
        conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
        conf.setBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, true);
        conf.set(HadoopConfTool.FS_HDFS_IMPL, HadoopConfTool.DEFAULT_FS_HDFS_IMPL);
    }
}
