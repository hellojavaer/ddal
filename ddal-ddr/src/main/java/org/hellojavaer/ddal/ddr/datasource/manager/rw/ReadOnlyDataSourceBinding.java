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
package org.hellojavaer.ddal.ddr.datasource.manager.rw;

import org.hellojavaer.ddal.ddr.datasource.WeightedDataSource;

import java.util.List;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class ReadOnlyDataSourceBinding {

    private String                   scNames;
    private List<WeightedDataSource> dataSources;

    public ReadOnlyDataSourceBinding() {
    }

    public ReadOnlyDataSourceBinding(String scNames, List<WeightedDataSource> dataSources) {
        this.scNames = scNames;
        this.dataSources = dataSources;
    }

    public String getScNames() {
        return scNames;
    }

    public void setScNames(String scNames) {
        this.scNames = scNames;
    }

    public List<WeightedDataSource> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<WeightedDataSource> dataSources) {
        this.dataSources = dataSources;
    }
}
