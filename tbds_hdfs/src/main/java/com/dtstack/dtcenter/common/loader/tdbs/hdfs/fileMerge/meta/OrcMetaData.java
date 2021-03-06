package com.dtstack.dtcenter.common.loader.tdbs.hdfs.fileMerge.meta;

import org.apache.orc.TypeDescription;

/**
 * orc文件的元信息
 */
public class OrcMetaData extends FileMetaData {
    //新的orc文件的schema
    private TypeDescription schema;

    public TypeDescription getSchema() {
        return schema;
    }

    public void setSchema(TypeDescription schema) {
        this.schema = schema;
    }
}
