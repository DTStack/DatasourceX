package com.dtstack.dtcenter.loader.dto.comparator;

import com.dtstack.dtcenter.loader.enums.Comparator;

import java.util.regex.Pattern;

/**
 * -
 *
 * @author ：wangchuan
 * date：Created in 下午5:50 2020/8/24
 * company: www.dtstack.com
 */
public class RegexStringComparator extends ByteArrayComparable {

    private EngineType engineType;

    private Integer flags;

    private String expr;

    public enum EngineType {
        JAVA,
        JONI
    }

    /**
     * Constructor
     * Adds Pattern.DOTALL to the underlying Pattern
     * @param expr a valid regular expression
     */
    public RegexStringComparator(String expr) {
        this(expr, Pattern.DOTALL);
    }

    /**
     * Constructor
     * Adds Pattern.DOTALL to the underlying Pattern
     * @param expr a valid regular expression
     * @param engine engine implementation type
     */
    public RegexStringComparator(String expr, EngineType engine) {
        this(expr, Pattern.DOTALL, engine);
    }

    /**
     * Constructor
     * @param expr a valid regular expression
     * @param flags java.util.regex.Pattern flags
     */
    public RegexStringComparator(String expr, int flags) {
        this(expr, flags, EngineType.JAVA);
    }

    /**
     * Constructor
     * @param expr a valid regular expression
     * @param flags java.util.regex.Pattern flags
     * @param engine engine implementation type
     */
    public RegexStringComparator(String expr, int flags, EngineType engine) {
        super(null);
        this.engineType = engine;
        this.expr = expr;
        this.flags = flags;
    }

    @Override
    public Integer getComparatorType() {
        return Comparator.REGEX_STRING.getVal();
    }

    public EngineType getEngineType() {
        return engineType;
    }

    public Integer getFlags() {
        return flags;
    }

    public String getExpr() {
        return expr;
    }
}
