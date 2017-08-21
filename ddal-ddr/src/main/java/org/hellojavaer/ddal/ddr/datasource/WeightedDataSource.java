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
package org.hellojavaer.ddal.ddr.datasource;

import org.hellojavaer.ddal.ddr.utils.DDRToStringBuilder;

import javax.sql.DataSource;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 23/11/2016.
 */
public class WeightedDataSource {

    private DataSource dataSource;
    private Integer    weight;
    private String     name;
    private String     desc;

    public WeightedDataSource() {
    }

    public WeightedDataSource(DataSource dataSource, Integer weight) {
        this(dataSource, weight, null);
    }

    public WeightedDataSource(DataSource dataSource, Integer weight, String name) {
        this(dataSource, weight, name, null);
    }

    public WeightedDataSource(DataSource dataSource, Integer weight, String name, String desc) {
        this.dataSource = dataSource;
        this.weight = weight;
        this.name = name;
        this.desc = desc;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return new DDRToStringBuilder().append("name", name).append("weight", weight).append("desc", desc).append("dataSource",
                                                                                                                  dataSource).toString();
    }
}
