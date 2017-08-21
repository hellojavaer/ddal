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
package org.hellojavaer.ddal.ddr.shard.simple;

import org.hellojavaer.ddal.ddr.shard.ShardRouteRule;
import org.hellojavaer.ddal.ddr.shard.ShardRouteRuleBinding;

import java.io.Serializable;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class SimpleShardRouteRuleBinding implements ShardRouteRuleBinding, Serializable {

    private static final long serialVersionUID = 0L;

    private String            scName;
    private String            tbName;
    private String            sdKey;
    private ShardRouteRule    rule;
    private String            sdValues;

    public SimpleShardRouteRuleBinding() {
    }

    public SimpleShardRouteRuleBinding(String scName, String tbName, String sdKey, ShardRouteRule rule) {
        this(scName, tbName, sdKey, rule, null);
    }

    public SimpleShardRouteRuleBinding(String scName, String tbName, String sdKey, ShardRouteRule rule, String sdValues) {
        this.scName = scName;
        this.tbName = tbName;
        this.sdKey = sdKey;
        this.rule = rule;
        this.sdValues = sdValues;
    }

    @Override
    public String getScName() {
        return scName;
    }

    public void setScName(String scName) {
        this.scName = scName;
    }

    @Override
    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    @Override
    public String getSdKey() {
        return sdKey;
    }

    public void setSdKey(String sdKey) {
        this.sdKey = sdKey;
    }

    @Override
    public ShardRouteRule getRule() {
        return rule;
    }

    public void setRule(ShardRouteRule rule) {
        this.rule = rule;
    }

    public String getSdValues() {
        return sdValues;
    }

    public void setSdValues(String sdValues) {
        this.sdValues = sdValues;
    }

}
