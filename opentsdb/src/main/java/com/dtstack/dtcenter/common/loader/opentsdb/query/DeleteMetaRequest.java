package com.dtstack.dtcenter.common.loader.opentsdb.query;

import java.util.List;
import java.util.Map;

public class DeleteMetaRequest extends Timeline {

    private boolean deleteData;

    private boolean recursive;

    public DeleteMetaRequest(String metric, Map<String, String> tags, boolean deleteData, boolean recursive) {
        super(metric, tags);
        this.deleteData = deleteData;
        this.recursive = recursive;
    }

    public DeleteMetaRequest(String metric, Map<String, String> tags, List<String> fields, boolean deleteData, boolean recursive) {
        super(metric, tags, fields);
        this.deleteData = deleteData;
        this.recursive = recursive;
    }

    public boolean isDeleteData() {
        return deleteData;
    }

    public void setDeleteData(boolean deleteData) {
        this.deleteData = deleteData;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }
}
