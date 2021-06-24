package com.dtstack.dtcenter.common.loader.opentsdb.query;

import com.dtstack.dtcenter.common.loader.opentsdb.type.JSONValue;

import java.util.List;
import java.util.Map;

public class MetricTimeRange extends JSONValue {

    private String metric;

    /**
     * @since v2.4.1 作为可选择选项 v2.3.0 不支持
     */
    private Map<String, String> tags;

    /**
     * 如果提供，我们只删除属于提供的字段的数据
     */
    private List<String> fields;

    private long start;

    private long end;

    public MetricTimeRange() {
        super();
    }

    public MetricTimeRange(String metric, long start, long end) {
        this(metric, null, null, start, end);
    }

    public MetricTimeRange(String metric, List<String> fields, long start, long end) {
        this(metric, null, fields, start, end);
    }

    public MetricTimeRange(String metric, Map<String, String> tags, long start, long end) {
        this(metric, tags, null, start, end);
    }

    public MetricTimeRange(String metric, Map<String, String> tags, List<String> fields, long start, long end) {
        super();
        this.metric = metric;
        this.tags = tags;
        this.fields = fields;
        this.start = start;
        this.end = end;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

}
