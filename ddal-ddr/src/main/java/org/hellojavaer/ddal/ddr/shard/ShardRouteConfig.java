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
package org.hellojavaer.ddal.ddr.shard;

import org.hellojavaer.ddal.ddr.utils.DDRToStringBuilder;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 20/12/2016.
 */
public class ShardRouteConfig {

    private String scName;
    private String tbName;
    private String sdKey;

    public ShardRouteConfig() {
    }

    public ShardRouteConfig(String scName, String tbName, String sdKey) {
        this.scName = scName;
        this.tbName = tbName;
        this.sdKey = sdKey;
    }

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

    @Override
    public String toString() {
        return new DDRToStringBuilder().append("scName", scName).append("tbName", tbName).append("sdKey", sdKey).toString();
    }
}
