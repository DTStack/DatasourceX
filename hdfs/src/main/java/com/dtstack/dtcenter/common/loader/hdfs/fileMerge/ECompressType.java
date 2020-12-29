package com.dtstack.dtcenter.common.loader.hdfs.fileMerge;

import org.apache.commons.lang.StringUtils;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/4/3
 */
public enum ECompressType {

    /**
     * text file
     */
    TEXT_GZIP("GZIP", "text", ".gz", 0.331F),
    TEXT_BZIP2("BZIP2", "text", ".bz2", 0.259F),
    TEXT_NONE("NONE", "text", "", 0.637F),

    /**
     * orc file
     */
    ORC_SNAPPY("SNAPPY", "orc", ".snappy", 0.233F),
    ORC_GZIP("GZIP", "orc", ".gz", 1.0F),
    ORC_BZIP("BZIP", "orc", ".bz", 1.0F),
    ORC_LZ4("LZ4", "orc", ".lz4", 1.0F),
    ORC_NONE("NONE", "orc", "", 0.233F),

    /**
     * parquet file
     */
    PARQUET_SNAPPY("SNAPPY", "parquet", ".snappy", 0.274F),
    PARQUET_GZIP("GZIP", "parquet", ".gz", 1.0F),
    PARQUET_LZO("LZO", "parquet", ".lzo", 1.0F),
    PARQUET_NONE("NONE", "parquet", "", 1.0F);

    private String type;

    private String fileType;

    private String suffix;

    private float deviation;

    ECompressType(String type, String fileType, String suffix, float deviation) {
        this.type = type;
        this.fileType = fileType;
        this.suffix = suffix;
        this.deviation = deviation;
    }

    public static ECompressType getByTypeAndFileType(String type, String fileType) {
        if (StringUtils.isEmpty(type)) {
            return ORC_NONE;
        }

        for (ECompressType value : ECompressType.values()) {
            if (value.getType().equalsIgnoreCase(type) && value.getFileType().equalsIgnoreCase(fileType)) {
                return value;
            }
        }
        return ORC_NONE;
    }

    public String getType() {
        return type;
    }

    public String getFileType() {
        return fileType;
    }

    public String getSuffix() {
        return suffix;
    }

    public float getDeviation() {
        return deviation;
    }
}
