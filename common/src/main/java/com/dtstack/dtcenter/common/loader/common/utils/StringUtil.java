package com.dtstack.dtcenter.common.loader.common.utils;

public class StringUtil {
    private static final char[] DIGITS_LOWER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static String encodeHex(byte[] data) {
        return encodeHex(data, DIGITS_LOWER);
    }

    public static String encodeHex(byte[] data, char[] toDigits) {
        int length = data.length;
        char[] out = new char[length * 3];
        int i = 0;
        for (int var = 0; i < length; ++i) {
            out[var++] = toDigits[(240 & data[i]) >>> 4];
            out[var++] = toDigits[15 & data[i]];
            out[var++] = ' ';
        }

        return String.valueOf(out);
    }
}
