package com.dtstack.dtcenter.loader.dto.filter;

import com.dtstack.dtcenter.loader.enums.HbaseFilterType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class FilterList extends Filter {

    public enum Operator {

        /**
         * ADN
         */
        MUST_PASS_ALL,

        /**
         * OR
         */
        MUST_PASS_ONE
    }

    /**
     * 默认使用 and
     */
    private Operator operator = Operator.MUST_PASS_ALL;

    private List<Filter> filters = new ArrayList<>();

    @Override
    public Integer getFilterType() {
        return HbaseFilterType.FILTER_LIST.getVal();
    }

    /**
     * 过滤器类型构造 {@link Filter}，默认类型为
     * MUST_PASS_ALL is assumed.
     *
     * @param rowFilters list of filters
     */
    public FilterList(final List<Filter> rowFilters) {
        if (rowFilters instanceof ArrayList) {
            this.filters = rowFilters;
        } else {
            this.filters = new ArrayList<>(rowFilters);
        }
    }

    /**
     * 过滤器类型构造.
     *
     * @param operator 过滤器类型 {@link Operator}.
     */
    public FilterList(final Operator operator) {
        this.operator = operator;
    }

    /**
     * operator 和 过滤器构造，支持集合.
     *
     * @param operator   过滤器类型.
     * @param rowFilters 过滤器列表.
     */
    public FilterList(final Operator operator, final List<Filter> rowFilters) {
        this.filters = new ArrayList<Filter>(rowFilters);
        this.operator = operator;
    }

    /**
     * operator 和 过滤器构造，支持可变参数.
     *
     * @param operator   过滤器类型.
     * @param rowFilters 过滤器.
     */
    public FilterList(final Operator operator, final Filter... rowFilters) {
        this.filters = new ArrayList<>(Arrays.asList(rowFilters));
        this.operator = operator;
    }

    /**
     * 获取过滤器类型 OR ｜ ADN.
     *
     * @return operator.
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * 获取过滤器列表.
     *
     * @return 过滤器列表.
     */
    public List<Filter> getFilters() {
        return filters;
    }

    /**
     * 添加一个过滤器.
     *
     * @param filter 过滤器.
     */
    public void addFilter(Filter filter) {
        this.filters.add(filter);
    }
}
