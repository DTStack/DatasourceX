package com.dtstack.dtcenter.loader.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlFormatUtil {
    private String sql;
    private static final String RETURN_REGEX = "\r";
    private static final String NEWLINE_REGEX = "\n";
    private static final String COMMENTS_REGEX = "--.*";
    private static final String COMMENTS_REGEX_LIBRA = "\\/\\*\\*+.*\\*\\*+\\/";
    private static final String COMMENTS_REGEX_LIBRA_SINGLE_QUOTATION_MARKS = "/\\*{1,2}\\*/";
    private static final String COMMENTS_REGEX_LIBRA_SINGLE_QUOTATION_MARKS2 = "/\\*{1,2}[\\s\\S]*?\\*/";
    private static final String LIFECYCLE_REGEX = "(?i)lifecycle\\s+(?<lifecycle>[1-9]\\d*)";
    private static final String CATALOGUE_REGEX = "(?i)catalogue\\s+(?<catalogue>[1-9]\\d*)";
    private static final String STORE_REGEX = "(?i)store\\s+(?<store>[a-Z]\\d*)";
    private static final String EMPTY_QUOTATION = "''";
    private static final String CREATE_REGEX = "(?i)create\\s+(external|temporary)*\\s*table\\s+[\\W\\w]+";
    private static final String DDL_REGEX = "(?i)(insert|create|drop|alter|truncate|set|update|delete)+\\s+[\\W\\w]+";
    private static final String SQL_SPLIT_REGEX = "('[^']*?')|(\"[^\"]*?\")";
    public static final String SPLIT_CHAR = ";";
    public static Pattern pattern = Pattern.compile("(?i)(map|struct|array)<");

    private SqlFormatUtil() {
    }

    public static boolean isDDLSql(String sql) {
        return sql.matches("(?i)(insert|create|drop|alter|truncate|set|update|delete)+\\s+[\\W\\w]+");
    }

    public static boolean isCreateSql(String sql) {
        return sql.matches("(?i)create\\s+(external|temporary)*\\s*table\\s+[\\W\\w]+");
    }

    public static List<String> splitSqlText(String sqlText) {
        String sqlTemp = sqlText;
        Pattern pattern = Pattern.compile("('[^']*?')|(\"[^\"]*?\")");

        String group;
        for(Matcher matcher = pattern.matcher(sqlText); matcher.find(); sqlTemp = sqlTemp.replace(group, StringUtils.repeat(" ", group.length()))) {
            group = matcher.group();
        }

        int pos;
        ArrayList posits;
        for(posits = Lists.newArrayList(); sqlTemp.contains(";"); sqlTemp = sqlTemp.substring(pos + 1)) {
            pos = sqlTemp.indexOf(";");
            posits.add(pos);
        }

        List<String> sqls = Lists.newArrayList();

        Integer posit;
        for(Iterator var6 = posits.iterator(); var6.hasNext(); sqlText = sqlText.substring(posit + 1)) {
            posit = (Integer)var6.next();
            sqls.add(sqlText.substring(0, posit));
        }

        return sqls;
    }


    public static SqlFormatUtil init(String sql) {
        SqlFormatUtil util = new SqlFormatUtil();
        util.sql = sql;
        return util;
    }

    public static String formatSql(String sql) {
        return init(sql).removeComment().toOneLine().removeBlank().removeEndChar().getSql();
    }

    public static String getStandardSql(String sql) {
        return init(sql).removeCatalogue().removeLifecycle().getSql();
    }

    public SqlFormatUtil toSingleSql() {
        if (this.sql.contains(";")) {
            this.sql = this.sql.split(";")[0];
        }

        return this;
    }

    public SqlFormatUtil toOneLine() {
        this.sql = this.sql.replaceAll("\r", " ").replaceAll("\n", " ");
        return this;
    }

    public SqlFormatUtil removeEndChar() {
        this.sql = this.sql.trim();
        if (this.sql.endsWith(";")) {
            this.sql = this.sql.substring(0, this.sql.length() - 1);
        }

        return this;
    }

    public SqlFormatUtil removeComment() {
        this.sql = this.sql.replaceAll("--.*", "");
        this.sql = this.sql.replaceAll("\\/\\*\\*+.*\\*\\*+\\/", "");
        this.sql = this.sql.replaceAll("/\\*{1,2}\\*/", "");
        this.sql = this.sql.replaceAll("/\\*{1,2}[\\s\\S]*?\\*/", "");
        return this;
    }

    public SqlFormatUtil removeCatalogue() {
        this.sql = this.sql.replaceAll("(?i)catalogue\\s+(?<catalogue>[1-9]\\d*)", "");
        return this;
    }

    public SqlFormatUtil removeLifecycle() {
        this.sql = this.sql.replaceAll("(?i)lifecycle\\s+(?<lifecycle>[1-9]\\d*)", "");
        return this;
    }

    public SqlFormatUtil removeBlank() {
        this.sql = this.sql.trim();
        return this;
    }

    public String getSql() {
        return this.sql;
    }

    public static String removeLimit(String sql) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        } else {
            String formattedSql = sql;
            Pattern compile = Pattern.compile("(?i)limit\\s+[0-9]+\\s*(\\,\\s*[0-9]+){0,1}");

            String group;
            for(Matcher matcher = compile.matcher(sql); matcher.find(); formattedSql = sql.replaceAll(group, " ")) {
                group = matcher.group(0);
            }

            return formattedSql;
        }
    }

    public static String removeComment(String sql) {
        return StringUtils.isBlank(sql) ? "" : sql.replaceAll("(?i)comment\\s*'([^']*)'", "");
    }

    public static String removeDoubleQuotesComment(String sql) {
        return StringUtils.isBlank(sql) ? "" : sql.replaceAll("(?i)comment\\s*\"([^\"]*)\"", "");
    }

    public static String formatType(String sql) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        } else {
            Matcher matcher = pattern.matcher(sql);
            if (!matcher.find()) {
                return sql;
            } else {
                String replace_type = sql.replaceAll("(?i)(map|struct|array)<", "replace_type<");
                Stack<String> stack = new Stack();
                String[] s = replace_type.split("replace_type");
                StringBuilder stringBuilder = new StringBuilder();
                boolean isBegin = false;

                for(int i = 0; i < s.length; ++i) {
                    String data = s[i];
                    if (data.contains("replace_type")) {
                        isBegin = true;
                    }

                    char[] var9 = data.toCharArray();
                    int var10 = var9.length;

                    for(int var11 = 0; var11 < var10; ++var11) {
                        char c = var9[var11];
                        String value = String.valueOf(c);
                        if ("<".equalsIgnoreCase(value)) {
                            stack.push(value);
                        } else if (">".equals(value)) {
                            stack.pop();
                            if (stack.isEmpty()) {
                                isBegin = false;
                            }
                        }

                        if (!isBegin && stack.isEmpty() && !">".equals(value)) {
                            stringBuilder.append(value);
                        }
                    }

                    if (i != s.length - 1 && !isBegin && stack.isEmpty()) {
                        stringBuilder.append(" string ");
                    }
                }

                return stringBuilder.toString();
            }
        }
    }
}
