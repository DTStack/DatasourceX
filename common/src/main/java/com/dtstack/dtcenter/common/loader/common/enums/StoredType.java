package com.dtstack.dtcenter.common.loader.common.enums;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:44 2020/9/12
 * @Description：表存储类型
 */
public enum StoredType {
    /**
     * LazySimpleSerDe
     */
    SEQUENCEFILE("LazySimpleSerDe", "SequenceFileInputFormat", "HiveSequenceFileOutputFormat"),

    /**
     * Default, depending on hive.default.fileformat configuration
     */
    TEXTFILE("LazySimpleSerDe", "TextInputFormat", "HiveIgnoreKeyTextOutputFormat"),

    /**
     * Note: Available in Hive 0.6.0 and later)
     */
    RCFILE("LazyBinaryColumnarSerDe", "RCFileInputFormat", "RCFileOutputFormat"),

    /**
     * Note: Available in Hive 0.11.0 and later
     */
    ORC("OrcSerde", "OrcInputFormat", "OrcOutputFormat"),

    /**
     * Note: Available in Hive 0.13.0 and later
     */
    PARQUET("ParquetHiveSerDe", "MapredParquetInputFormat", "MapredParquetOutputFormat"),

    /**
     * Note: Available in Hive 0.14.0 and later)
     */
    AVRO("AvroSerDe", "AvroContainerInputFormat", "AvroContainerOutputFormat"),

    /**
     * KUDU
     */
    KUDU("KUDU","KuduInputFormat","KuduOutputFormat"),

    /**
     * 未知存储类型
     */
    UN_KNOW_TYPE("unknown stored type","unknown stored type","unknown stored type"),
    ;

    private String serde;
    private String inputFormatClass;
    private String outputFormatClass;

    StoredType(String serde, String inputFormatClass, String outputFormatClass) {
        this.serde = serde;
        this.inputFormatClass = inputFormatClass;
        this.outputFormatClass = outputFormatClass;
    }

    public String getSerde() {
        return serde;
    }

    public String getInputFormatClass() {
        return inputFormatClass;
    }
}
