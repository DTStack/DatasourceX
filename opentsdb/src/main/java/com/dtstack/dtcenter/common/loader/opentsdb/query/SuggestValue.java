package com.dtstack.dtcenter.common.loader.opentsdb.query;


import com.dtstack.dtcenter.common.loader.opentsdb.type.JSONValue;

public class SuggestValue extends JSONValue {
    private String type;
    private String metric;
    private String q;
    private int max;

    public SuggestValue() {
        super();
    }

    public SuggestValue(String type, String q, int max) {
        super();
        this.type = type;
        this.q = q;
        this.max = max;
    }

    public SuggestValue(String type, String metric, String q, int max) {
        super();
        this.type = type;
        this.metric = metric;
        this.q = q;
        this.max = max;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getQ() {
        return q;
    }

    public void setQ(String q) {
        this.q = q;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

}
