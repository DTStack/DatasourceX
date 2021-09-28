/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
