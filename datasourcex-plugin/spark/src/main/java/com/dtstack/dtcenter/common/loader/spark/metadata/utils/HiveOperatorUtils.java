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

package com.dtstack.dtcenter.common.loader.spark.metadata.utils;

import com.dtstack.dtcenter.common.loader.spark.metadata.constants.StMetaDataCons;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveOperatorUtils {

    public static Pattern numberPattern = Pattern.compile("([1-9]\\d*\\.?\\d*)|0");


    public static String getStoredType(String storedClass) {
        if (storedClass.endsWith(StMetaDataCons.TEXT_FORMAT)){
            return StMetaDataCons.TYPE_TEXT;
        } else if (storedClass.endsWith(StMetaDataCons.ORC_FORMAT)){
            return StMetaDataCons.TYPE_ORC;
        } else if (storedClass.endsWith(StMetaDataCons.PARQUET_FORMAT)){
            return StMetaDataCons.TYPE_PARQUET;
        } else {
            return storedClass;
        }
    }

    public static Map<String, String> parseToMap(String str){
        HashMap<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(str) && str.length() > 2){
            String temp = str.substring(1, str.length() - 1);
            String[] split = temp.split(", ");
            for (String s : split){
                if(StringUtils.isNotEmpty(s)){
                    String[] keyAndValue = s.split("=");
                    map.put(keyAndValue[0], keyAndValue[1]);
                }
            }
        }
        return map;
    }



    //Statistics: like sizeInBytes=17968503, rowCount=78444, isBroadcastable=false
    //Statistics  like 72 bytes, 1 rows
    public static Map<String, String> parseStatisticsColonToMap(String str){
        HashMap<String, String> map = new HashMap<>();
        if (StringUtils.isNotEmpty(str)){
            if(str.contains("bytes") && str.contains("rows")){
                Matcher matcher = numberPattern.matcher(str);

                int index = 0;
                while (matcher.find()){
                    String group = matcher.group();
                    if(index == 0){
                        map.put(StMetaDataCons.KEY_STATISTICS_SIZEINBYTES, group);
                    }
                    if(index == 1){
                        map.put(StMetaDataCons.KEY_STATISTICS_ROWCOUNT, group);
                    }
                    index++;
                }
            }else{
                String[] split = str.split(",");
                for (String s : split){
                    if(StringUtils.isNotEmpty(s)){
                        String[] keyAndValue = s.split("=");
                        map.put(keyAndValue[0].trim(), keyAndValue[1].trim());
                    }
                }
            }
        }
        return map;
    }
    /**
     * Convert time string similar to Fri Jun 18 16:57:02 CST 2021
     * and Fri Jun 18 16:57:02 GMT+08:00 2021 into timestamp
     * @param str time string
     * @return
     */
    public static String parseToTimestamp(String str){
        if(StringUtils.isNotEmpty(str)){
            SimpleDateFormat sdfBase = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
            SimpleDateFormat sdfOne =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sdfTwo =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (str.contains("CST")){
                try {
                    // The time represented by str is China Standard Time UT+8:00,
                    // but sdf_base regards it as use Central Standard Time (USA) UT-6:00.
                    // When the parse method is executed (conversion to local time GMT+8:00),
                    // 14 will be added by default Hours, this is what we don’t need.
                    Date dateOne = sdfBase.parse(str);
                    //So we set the time zone to GMT-6:00 (14 hours difference from GMT+8:00)
                    sdfOne.setTimeZone(TimeZone.getTimeZone("GMT-06:00"));
                    Date dateTwo = sdfTwo.parse(sdfOne.format(dateOne));
                    //Date to timestamp (seconds)
                    return String.valueOf(dateTwo.getTime() / 1000);
                } catch (ParseException e) {
                    return "";
                }
            }else{
                try {
                    Date dateOne = sdfBase.parse(str);
                    Date dateTwo = sdfTwo.parse(sdfOne.format(dateOne));
                    return String.valueOf(dateTwo.getTime() / 1000);
                } catch (ParseException e) {
                    return "";
                }
            }
        }
        return "";
    }
}
