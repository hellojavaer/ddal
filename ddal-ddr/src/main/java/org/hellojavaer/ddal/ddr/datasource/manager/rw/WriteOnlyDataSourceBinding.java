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

import javax.sql.DataSource;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class WriteOnlyDataSourceBinding {

    private String     scNames;
    private DataSource dataSource;

    public WriteOnlyDataSourceBinding() {
    }

    public WriteOnlyDataSourceBinding(String scNames, DataSource dataSource) {
        this.scNames = scNames;
        this.dataSource = dataSource;
    }

    public String getScNames() {
        return scNames;
    }

    public void setScNames(String scNames) {
        this.scNames = scNames;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
