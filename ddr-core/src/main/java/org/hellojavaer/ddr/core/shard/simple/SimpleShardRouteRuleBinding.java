/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hellojavaer.ddr.core.shard.simple;

import java.io.Serializable;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleShardRouteRuleBinding implements Serializable {

    public static final String   VALUE_TYPE_OF_NUMBER = "number";
    public static final String   VALUE_TYPE_OF_STRING = "string";

    private String               scName;
    private String               tbName;
    private String               sdKey;
    private SimpleShardRouteRule rule;
    private String               sdValues;
    private String               sdValueType          = VALUE_TYPE_OF_NUMBER;

    public String getScName() {
        return scName;
    }

    public void setScName(String scName) {
        this.scName = scName;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getSdKey() {
        return sdKey;
    }

    public void setSdKey(String sdKey) {
        this.sdKey = sdKey;
    }

    public SimpleShardRouteRule getRule() {
        return rule;
    }

    public void setRule(SimpleShardRouteRule rule) {
        this.rule = rule;
    }

    public String getSdValues() {
        return sdValues;
    }

    public void setSdValues(String sdValues) {
        this.sdValues = sdValues;
    }

    public String getSdValueType() {
        return sdValueType;
    }

    public void setSdValueType(String sdValueType) {
        this.sdValueType = sdValueType;
    }
}
