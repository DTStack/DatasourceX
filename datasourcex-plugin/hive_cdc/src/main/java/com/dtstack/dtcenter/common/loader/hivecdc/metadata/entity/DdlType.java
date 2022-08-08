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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.entity;

public enum DdlType {

    CREATE_DATABASE("CREATE_DATABASE","CREATE"),
    DROP_DATABASE("DROP_DATABASE","DROP"),
    CREATE_TABLE("CREATE_TABLE","CREATE"),
    ALTER_TABLE("ALTER_TABLE","ALTER"),
    RENAME_TABLE("RENAME_TABLE","ALTER"),
    DROP_TABLE("DROP_TABLE","DROP"),
    ADD_PARTITION("ADD_PARTITION","ALTER"),
    DROP_PARTITION("DROP_PARTITION","ALTER"),
    ALTER_PARTITION("ALTER_PARTITION","ALTER"),
    UNKNOWN("UNKNOWN","ALTER")
    ;

    private final String value;

    private final String type;

    public String getValue() {
        return value;
    }

    public String getType() {
        return type;
    }

    DdlType(String value, String type) {
        this.value =value;
        this.type = type;
    }

    public static DdlType valuesOf(String value) {
        DdlType[] eventTypes = values();
        for (DdlType eventType : eventTypes) {
            if (eventType.value.equalsIgnoreCase(value)) {
                return eventType;
            }
        }
        return UNKNOWN;
    }

}
