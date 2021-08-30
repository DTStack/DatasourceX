package com.dtstack.dtcenter.common.loader.common.utils;

public class StringUtil {

    private static final char[] DIGITS_UPPER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static char[] encodeHex(byte[] data) {
        return encodeHex(data, DIGITS_UPPER);
    }

    public static char[] encodeHex(byte[] data, char[] toDigits) {
        int l = data.length;
        char[] out = new char[l * 3];
        int i = 0;

        for (int var5 = 0; i < l; ++i) {
            out[var5++] = toDigits[(240 & data[i]) >>> 4];
            out[var5++] = toDigits[15 & data[i]];
            out[var5++] = ' ';
        }

        return out;
    }
}
