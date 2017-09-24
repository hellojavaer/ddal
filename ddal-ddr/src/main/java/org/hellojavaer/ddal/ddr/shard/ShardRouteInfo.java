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

import java.util.Objects;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 15/11/2016.
 */
public class ShardRouteInfo {

    private String scName;
    private String tbName;

    public ShardRouteInfo() {
    }

    public ShardRouteInfo(String scName, String tbName) {
        this.scName = scName;
        this.tbName = tbName;
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

    @Override
    public String toString() {
        if (scName == null) {
            return tbName;
        } else {
            return new StringBuilder().append(scName).append('.').append(tbName).toString();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(scName, tbName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardRouteInfo routeInfo = (ShardRouteInfo) o;

        if (scName != null ? !scName.equals(routeInfo.scName) : routeInfo.scName != null) return false;
        return tbName != null ? tbName.equals(routeInfo.tbName) : routeInfo.tbName == null;

    }
}
