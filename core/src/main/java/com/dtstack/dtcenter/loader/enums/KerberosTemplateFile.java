package com.dtstack.dtcenter.loader.enums;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:47 2020/8/25
 * @Description：Kerberos 模板文件
 */
public enum KerberosTemplateFile {
    HDFS(DataSourceType.HDFS.getVal(), "hdfs-kerberos.xml"),
    HIVE1(DataSourceType.HIVE1X.getVal(), "hive-kerberos.xml"),
    HIVE(DataSourceType.HIVE.getVal(), "hive-kerberos.xml"),
    HBASE(DataSourceType.HBASE.getVal(), "hbase-kerberos.xml"),
    HBASE2(DataSourceType.HBASE2.getVal(), "hbase-kerberos.xml"),
    KUDU(DataSourceType.Kudu.getVal(), "kudu-kerberos.xml"),
    IMPALA(DataSourceType.IMPALA.getVal(), "impala-kerberos.xml"),
    KAFKA(DataSourceType.KAFKA.getVal(), "kafka-kerberos.xml"),
    KAFKA_11(DataSourceType.KAFKA_11.getVal(), "kafka-kerberos.xml"),
    KAFKA_10(DataSourceType.KAFKA_10.getVal(), "kafka-kerberos.xml"),
    KAFKA_09(DataSourceType.KAFKA_09.getVal(), "kafka-kerberos.xml"),
    KAFKA_2X(DataSourceType.KAFKA_2X.getVal(), "kafka-kerberos.xml"),
    ;

    /**
     * 数据源类型
     */
    private Integer sourceType;

    /**
     * 模板文件名称
     */
    private String fileName;

    KerberosTemplateFile(Integer sourceType, String fileName) {
        this.sourceType = sourceType;
        this.fileName = fileName;
    }

    /**
     * 获取数据源类型
     *
     * @return
     */
    public Integer getSourceType() {
        return sourceType;
    }

    /**
     * 获取模板文件名称
     *
     * @return
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * 根据数据源类型获取模板文件名称
     *
     * @param sourceType
     * @return
     */
    public static String getFileName(Integer sourceType) {
        for (KerberosTemplateFile templateFile : KerberosTemplateFile.values()) {
            if (templateFile.getSourceType().equals(sourceType)) {
                return templateFile.getFileName();
            }
        }

        throw new DtLoaderException("对应数据源模板文件不存在");
    }
}
