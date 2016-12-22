/*
 * Copyright 2016-2016 the original author or authors.
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
package org.hellojavaer.ddr.core.datasource.manager.rw.monitor.mbean;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 21/12/2016.
 */
public class ReadOnlyDataSourceServerMonitor implements ReadOnlyDataSourceServerMonitorMXBean {

    @Override
    public Integer getWeight(String scName, int index) {
        return null;
    }

    @Override
    public String setWeight(String scName, int index, int weight) {
        return null;
    }

    @Override
    public String restoreWeight(String scName, int index) {
        return null;
    }

    @Override
    public Integer getWeight(String scName, String name) {
        return null;
    }

    @Override
    public String setWeight(String scName, String name, int weight) {
        return null;
    }

    @Override
    public String restoreWeight(String scName, String name) {
        return null;
    }

    @Override
    public String getWeight(String scName) {
        return null;
    }

    @Override
    public String setWeight(String scName, String values) {
        return null;
    }

    @Override
    public String restoreWeight(String scName) {
        return null;
    }

    @Override
    public String restoreWeight() {
        return null;
    }

    @Override
    public String getOriginalWeightConfig() {
        return null;
    }

    @Override
    public String getCurrentWeightConfig() {
        return null;
    }
}
